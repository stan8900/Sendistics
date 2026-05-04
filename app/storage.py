import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import uuid4

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - driver optional
    psycopg = None
    dict_row = None


class Storage:
    def __init__(
        self,
        path: Optional[Path],
        *,
        legacy_json_path: Optional[Path] = None,
        database_url: Optional[str] = None,
    ) -> None:
        self._path = path
        self._legacy_json = legacy_json_path
        self._database_url = database_url
        self._is_postgres = bool(database_url)
        self._lock = asyncio.Lock()
        if self._is_postgres:
            if not database_url:
                raise ValueError("DATABASE_URL must be provided for PostgreSQL storage.")
            if psycopg is None:
                raise RuntimeError("psycopg is required for PostgreSQL storage. Install psycopg[binary].")
            self._conn = psycopg.connect(database_url, autocommit=True, row_factory=dict_row)
        else:
            if path is None:
                raise ValueError("Storage path is required when DATABASE_URL is not set.")
            self._legacy_json = legacy_json_path or path.with_suffix(".json")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        self._init_db()
        if (
            not self._is_postgres
            and self._legacy_json
            and self._path
            and self._legacy_json != self._path
            and self._legacy_json.exists()
            and not self._has_any_data()
        ):
            self._migrate_from_json(self._legacy_json)

    def _prepare_query(self, query: str) -> str:
        if not self._is_postgres:
            return query
        return query.replace("?", "%s")

    def _execute(self, query: str, params: Sequence[Any] = ()) -> Any:
        sql = self._prepare_query(query)
        return self._conn.execute(sql, params)

    def _executemany(self, query: str, seq_of_params: Iterable[Sequence[Any]]) -> Any:
        sql = self._prepare_query(query)
        if self._is_postgres:
            with self._conn.cursor() as cur:
                cur.executemany(sql, seq_of_params)
                return cur
        return self._conn.executemany(sql, seq_of_params)

    def _commit(self) -> None:
        if not self._is_postgres:
            self._conn.commit()

    def _column_exists(self, table: str, column: str) -> bool:
        if self._is_postgres:
            query = """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s
            """
            cur = self._execute(query, (table, column))
            return cur.fetchone() is not None
        cur = self._conn.execute(f"PRAGMA table_info({table})")
        return any(row[1] == column for row in cur.fetchall())

    def _add_column_if_missing(self, table: str, column: str, definition: str) -> None:
        if self._column_exists(table, column):
            return
        sql = f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
        self._execute(sql)
        self._commit()

    async def get_data(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "auto": self._list_all_auto_locked(),
                "known_chats": self._list_known_chats_locked(),
                "payments": self._list_payments_locked(),
                "sessions": self._list_sessions_locked(),
            }

    async def get_auto(self, user_id: int) -> Dict[str, Any]:
        async with self._lock:
            return self._get_auto_locked(user_id)

    async def set_auto_message(self, user_id: int, message: str) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            self._execute(
                "UPDATE user_auto_configs SET message = ? WHERE user_id = ?",
                (message, user_id),
            )
            self._commit()

    async def set_auto_interval(self, user_id: int, minutes: int) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            self._execute(
                "UPDATE user_auto_configs SET interval_minutes = ? WHERE user_id = ?",
                (minutes, user_id),
            )
            self._commit()

    async def set_auto_enabled(self, user_id: int, enabled: bool) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            self._execute(
                "UPDATE user_auto_configs SET is_enabled = ? WHERE user_id = ?",
                (1 if enabled else 0, user_id),
            )
            self._commit()

    async def disable_all_auto(self) -> int:
        async with self._lock:
            row = self._execute(
                "SELECT COUNT(*) AS cnt FROM user_auto_configs WHERE is_enabled = 1"
            ).fetchone()
            disabled_count = int(row["cnt"] or 0) if row else 0
            self._execute("UPDATE user_auto_configs SET is_enabled = 0 WHERE is_enabled = 1")
            self._execute("UPDATE auto_config SET is_enabled = 0 WHERE id = 1")
            self._commit()
            return disabled_count

    async def reserve_auto_delivery(
        self,
        *,
        user_id: int,
        chat_id: int,
        day_key: str,
        now_iso: str,
        daily_limit: int,
        chat_interval_seconds: int,
    ) -> Tuple[bool, str]:
        async with self._lock:
            daily_row = self._execute(
                """
                SELECT sent_count
                FROM auto_daily_limits
                WHERE user_id = ? AND day_key = ?
                """,
                (user_id, day_key),
            ).fetchone()
            sent_count = int(daily_row["sent_count"] or 0) if daily_row else 0
            if sent_count >= daily_limit:
                return False, "daily_limit"

            chat_row = self._execute(
                """
                SELECT last_sent_at
                FROM auto_chat_rate_limits
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchone()
            if chat_row and chat_row["last_sent_at"]:
                try:
                    last_sent_at = datetime.fromisoformat(chat_row["last_sent_at"])
                    now_dt = datetime.fromisoformat(now_iso)
                    elapsed = (now_dt - last_sent_at).total_seconds()
                except (TypeError, ValueError):
                    elapsed = chat_interval_seconds
                if elapsed < chat_interval_seconds:
                    return False, "chat_rate_limit"

            self._execute(
                """
                INSERT INTO auto_daily_limits (user_id, day_key, sent_count)
                VALUES (?, ?, 1)
                ON CONFLICT(user_id, day_key) DO UPDATE SET
                    sent_count = auto_daily_limits.sent_count + 1
                """,
                (user_id, day_key),
            )
            self._execute(
                """
                INSERT INTO auto_chat_rate_limits (chat_id, last_sent_at)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    last_sent_at = excluded.last_sent_at
                """,
                (chat_id, now_iso),
            )
            self._commit()
            return True, "reserved"

    async def record_auto_campaign_start(
        self,
        user_id: int,
        *,
        started_at: Optional[str] = None,
    ) -> None:
        async with self._lock:
            self._execute(
                """
                INSERT INTO auto_campaign_events (id, user_id, started_at)
                VALUES (?, ?, ?)
                """,
                (uuid4().hex, user_id, started_at or datetime.utcnow().isoformat()),
            )
            self._commit()

    async def count_auto_campaign_starts(self, *, since: Optional[datetime] = None) -> int:
        async with self._lock:
            if since is None:
                row = self._execute(
                    "SELECT COUNT(*) AS cnt FROM auto_campaign_events"
                ).fetchone()
            else:
                row = self._execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM auto_campaign_events
                    WHERE started_at >= ?
                    """,
                    (since.isoformat(),),
                ).fetchone()
            return int(row["cnt"] or 0) if row else 0

    async def count_auto_deliveries(self, *, since: Optional[datetime] = None) -> int:
        async with self._lock:
            if since is None:
                row = self._execute(
                    "SELECT COALESCE(SUM(sent_count), 0) AS cnt FROM auto_delivery_events"
                ).fetchone()
            else:
                row = self._execute(
                    """
                    SELECT COALESCE(SUM(sent_count), 0) AS cnt
                    FROM auto_delivery_events
                    WHERE delivered_at >= ?
                    """,
                    (since.isoformat(),),
                ).fetchone()
            return int(row["cnt"] or 0) if row else 0

    async def count_active_auto_campaigns(self) -> int:
        async with self._lock:
            row = self._execute(
                "SELECT COUNT(*) AS cnt FROM user_auto_configs WHERE is_enabled = 1"
            ).fetchone()
            return int(row["cnt"] or 0) if row else 0

    async def list_auto_campaign_events(self, *, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if since is None:
                rows = self._execute(
                    """
                    SELECT user_id, started_at
                    FROM auto_campaign_events
                    ORDER BY started_at DESC
                    """
                ).fetchall()
            else:
                rows = self._execute(
                    """
                    SELECT user_id, started_at
                    FROM auto_campaign_events
                    WHERE started_at >= ?
                    ORDER BY started_at DESC
                    """,
                    (since.isoformat(),),
                ).fetchall()
            return [dict(row) for row in rows]

    async def list_auto_delivery_events(self, *, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if since is None:
                rows = self._execute(
                    """
                    SELECT user_id, sent_count, delivered_at
                    FROM auto_delivery_events
                    ORDER BY delivered_at DESC
                    """
                ).fetchall()
            else:
                rows = self._execute(
                    """
                    SELECT user_id, sent_count, delivered_at
                    FROM auto_delivery_events
                    WHERE delivered_at >= ?
                    ORDER BY delivered_at DESC
                    """,
                    (since.isoformat(),),
                ).fetchall()
            return [dict(row) for row in rows]

    async def toggle_target_chat(
        self,
        user_id: int,
        chat_id: int,
        title: Optional[str] = None,
        *,
        account_id: Optional[int] = None,
    ) -> bool:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            if account_id is None:
                cur = self._execute(
                    "SELECT 1 FROM user_auto_targets WHERE user_id = ? AND chat_id = ?",
                    (user_id, chat_id),
                )
                exists = cur.fetchone() is not None
                if exists:
                    self._execute(
                        "DELETE FROM user_auto_targets WHERE user_id = ? AND chat_id = ?",
                        (user_id, chat_id),
                    )
                    self._commit()
                    return False
                self._execute(
                    """
                    INSERT INTO user_auto_targets (user_id, chat_id)
                    VALUES (?, ?)
                    ON CONFLICT(user_id, chat_id) DO NOTHING
                    """,
                    (user_id, chat_id),
                )
                if title:
                    self._ensure_known_chat_locked(chat_id, title)
                self._commit()
                return True

            cur = self._execute(
                """
                SELECT 1 FROM user_account_targets
                WHERE user_id = ? AND account_id = ? AND chat_id = ?
                """,
                (user_id, account_id, chat_id),
            )
            exists = cur.fetchone() is not None
            if exists:
                self._execute(
                    """
                    DELETE FROM user_account_targets
                    WHERE user_id = ? AND account_id = ? AND chat_id = ?
                    """,
                    (user_id, account_id, chat_id),
                )
                self._commit()
                return False
            self._execute(
                """
                INSERT INTO user_account_targets (user_id, account_id, chat_id)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, account_id, chat_id) DO NOTHING
                """,
                (user_id, account_id, chat_id),
            )
            self._commit()
            return True

    async def set_target_chats(
        self,
        user_id: int,
        chat_ids: Iterable[int],
        *,
        account_id: Optional[int] = None,
    ) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            ids = []
            for chat_id in chat_ids:
                try:
                    ids.append(int(chat_id))
                except (TypeError, ValueError):
                    continue
            if account_id is None:
                self._execute("DELETE FROM user_auto_targets WHERE user_id = ?", (user_id,))
                target_table = "user_auto_targets"
                params = [(user_id, chat_id) for chat_id in ids]
            else:
                self._execute(
                    "DELETE FROM user_account_targets WHERE user_id = ? AND account_id = ?",
                    (user_id, account_id),
                )
                target_table = "user_account_targets"
                params = [(user_id, account_id, chat_id) for chat_id in ids]
            if ids:
                self._executemany(
                    {
                        "user_auto_targets": """
                            INSERT INTO user_auto_targets (user_id, chat_id)
                            VALUES (?, ?)
                            ON CONFLICT(user_id, chat_id) DO NOTHING
                        """,
                        "user_account_targets": """
                            INSERT INTO user_account_targets (user_id, account_id, chat_id)
                            VALUES (?, ?, ?)
                            ON CONFLICT(user_id, account_id, chat_id) DO NOTHING
                        """,
                    }[target_table],
                    params,
                )
            self._commit()

    async def clear_target_chats(self, user_id: int, *, account_id: Optional[int] = None) -> None:
        await self.set_target_chats(user_id, [], account_id=account_id)

    async def list_user_accounts(self, owner_id: int) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                """
                SELECT id, owner_user_id, phone, session, title, username, last_synced_at,
                       proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
                       created_at, updated_at
                FROM user_accounts
                WHERE owner_user_id = ?
                ORDER BY created_at DESC
                """,
                (owner_id,),
            ).fetchall()
            return [self._row_to_account(row) for row in rows]

    async def get_user_account(
        self,
        account_id: int,
        *,
        owner_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            row = self._execute(
                """
                SELECT id, owner_user_id, phone, session, title, username, last_synced_at,
                       proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
                       created_at, updated_at
                FROM user_accounts
                WHERE id = ?
                """,
                (account_id,),
            ).fetchone()
            if not row:
                return None
            account = self._row_to_account(row)
            if owner_id is not None and account["owner_user_id"] != owner_id:
                return None
            return account

    async def create_user_account(
        self,
        owner_id: int,
        *,
        phone: str,
        session: str,
        title: Optional[str],
        username: Optional[str],
    ) -> Dict[str, Any]:
        async with self._lock:
            now = datetime.utcnow().isoformat()
            if self._is_postgres:
                cur = self._execute(
                    """
                    INSERT INTO user_accounts (
                        owner_user_id, phone, session, title, username, last_synced_at,
                        proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        owner_id,
                        phone,
                        session,
                        title,
                        username,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        now,
                        now,
                    ),
                )
                new_id = cur.fetchone()["id"]
            else:
                cur = self._execute(
                    """
                    INSERT INTO user_accounts (
                        owner_user_id, phone, session, title, username, last_synced_at,
                        proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        owner_id,
                        phone,
                        session,
                        title,
                        username,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        now,
                        now,
                    ),
                )
                new_id = cur.lastrowid
            self._commit()
            account_id = int(new_id)
        return await self.get_user_account(account_id)

    async def delete_user_account(self, owner_id: int, account_id: int) -> bool:
        async with self._lock:
            row = self._execute(
                "SELECT owner_user_id FROM user_accounts WHERE id = ?",
                (account_id,),
            ).fetchone()
            if not row or int(row["owner_user_id"]) != int(owner_id):
                return False
            self._execute("DELETE FROM user_accounts WHERE id = ?", (account_id,))
            # Targets and chats are removed by cascading foreign keys
            self._execute(
                """
                UPDATE user_auto_configs
                SET sender_account_id = NULL
                WHERE sender_account_id = ?
                """,
                (account_id,),
            )
            self._commit()
            return True

    async def update_user_account_proxy(
        self,
        owner_id: int,
        account_id: int,
        *,
        proxy: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            row = self._execute(
                "SELECT owner_user_id FROM user_accounts WHERE id = ?",
                (account_id,),
            ).fetchone()
            if not row or int(row["owner_user_id"]) != int(owner_id):
                return None
            now = datetime.utcnow().isoformat()
            params = (
                proxy.get("type") if proxy else None,
                proxy.get("host") if proxy else None,
                int(proxy.get("port")) if proxy and proxy.get("port") is not None else None,
                proxy.get("username") if proxy else None,
                proxy.get("password") if proxy else None,
                now,
                account_id,
            )
            self._execute(
                """
                UPDATE user_accounts
                SET proxy_type = ?, proxy_host = ?, proxy_port = ?, proxy_username = ?, proxy_password = ?, updated_at = ?
                WHERE id = ?
                """,
                params,
            )
            self._commit()
        return await self.get_user_account(account_id)

    async def register_audience_dump(
        self,
        owner_user_id: int,
        *,
        source: str,
        file_path: str,
        total_users: int,
    ) -> Dict[str, Any]:
        async with self._lock:
            dump_id = uuid4().hex
            created_at = datetime.utcnow().isoformat()
            self._execute(
                """
                INSERT INTO audience_dumps (
                    id, owner_user_id, source, file_path, total_users, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (dump_id, owner_user_id, source, file_path, int(total_users), created_at),
            )
            self._commit()
            return self._fetch_audience_dump_locked(dump_id)

    async def list_audience_dumps(self, owner_user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                """
                SELECT id, owner_user_id, source, file_path, total_users, created_at
                FROM audience_dumps
                WHERE owner_user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (owner_user_id, max(1, int(limit))),
            ).fetchall()
            return [self._row_to_audience_dump(row) for row in rows]

    async def create_invite_job(
        self,
        owner_user_id: int,
        *,
        target_chat: str,
        usernames_file: str,
        settings: Dict[str, Any],
        total_users: int,
    ) -> Dict[str, Any]:
        async with self._lock:
            job_id = uuid4().hex
            created_at = datetime.utcnow().isoformat()
            self._execute(
                """
                INSERT INTO invite_jobs (
                    id, owner_user_id, target_chat, usernames_file,
                    status, total_users, invited_count, failed_count,
                    last_error, settings_json, created_at
                ) VALUES (?, ?, ?, ?, 'pending', ?, 0, 0, NULL, ?, ?)
                """,
                (
                    job_id,
                    owner_user_id,
                    target_chat,
                    usernames_file,
                    int(total_users),
                    json.dumps(settings, ensure_ascii=False),
                    created_at,
                ),
            )
            self._commit()
            return self._fetch_invite_job_locked(job_id)

    async def update_invite_job(self, job_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
        allowed = {
            "status",
            "invited_count",
            "failed_count",
            "last_error",
            "started_at",
            "finished_at",
            "settings",
        }
        updates: List[str] = []
        params: List[Any] = []
        for key, value in fields.items():
            if key not in allowed:
                continue
            column = "settings_json" if key == "settings" else key
            updates.append(f"{column} = ?")
            if key == "settings":
                params.append(json.dumps(value, ensure_ascii=False))
            else:
                params.append(value)
        if not updates:
            return await self.get_invite_job(job_id)
        async with self._lock:
            self._execute(
                f"""
                UPDATE invite_jobs
                SET {', '.join(updates)}
                WHERE id = ?
                """,
                (*params, job_id),
            )
            self._commit()
            return self._fetch_invite_job_locked(job_id)

    async def list_invite_jobs(self, owner_user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                """
                SELECT *
                FROM invite_jobs
                WHERE owner_user_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (owner_user_id, max(1, int(limit))),
            ).fetchall()
            return [self._row_to_invite_job(row) for row in rows]

    async def get_invite_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return self._fetch_invite_job_locked(job_id)

    async def update_user_account_session(
        self,
        owner_id: int,
        account_id: int,
        session: str,
    ) -> bool:
        async with self._lock:
            row = self._execute(
                "SELECT owner_user_id FROM user_accounts WHERE id = ?",
                (account_id,),
            ).fetchone()
            if not row or int(row["owner_user_id"]) != int(owner_id):
                return False
            now = datetime.utcnow().isoformat()
            self._execute(
                "UPDATE user_accounts SET session = ?, updated_at = ? WHERE id = ?",
                (session, now, account_id),
            )
            self._commit()
            return True

    async def set_user_sender_account(self, user_id: int, account_id: Optional[int]) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            if account_id is not None:
                owner_row = self._execute(
                    """
                    SELECT owner_user_id FROM user_accounts
                    WHERE id = ?
                    """,
                    (account_id,),
                ).fetchone()
                if not owner_row or int(owner_row["owner_user_id"]) != int(user_id):
                    raise ValueError("Аккаунт не найден или недоступен.")
            self._execute(
                "UPDATE user_auto_configs SET sender_account_id = ? WHERE user_id = ?",
                (account_id, user_id),
            )
            self._commit()

    async def replace_account_chats(
        self,
        account_id: int,
        chats: Iterable[Tuple[int, str]],
    ) -> None:
        async with self._lock:
            self._execute("DELETE FROM user_account_chats WHERE account_id = ?", (account_id,))
            to_insert = []
            for chat_id, title in chats:
                try:
                    chat_id_int = int(chat_id)
                except (TypeError, ValueError):
                    continue
                to_insert.append((account_id, chat_id_int, (title or f"Чат {chat_id_int}").strip()))
            if to_insert:
                self._executemany(
                    """
                    INSERT INTO user_account_chats (account_id, chat_id, title)
                    VALUES (?, ?, ?)
                    ON CONFLICT(account_id, chat_id) DO UPDATE SET title = excluded.title
                    """,
                    to_insert,
                )
            now = datetime.utcnow().isoformat()
            self._execute(
                "UPDATE user_accounts SET last_synced_at = ?, updated_at = ? WHERE id = ?",
                (now, now, account_id),
            )
            self._commit()

    async def list_account_chats(
        self,
        owner_id: int,
        account_id: int,
    ) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return self._list_user_account_chats_locked(account_id, owner_id=owner_id)

    async def update_stats(
        self,
        user_id: int,
        *,
        sent: int,
        errors: List[str],
        delivered_at: Optional[str] = None,
    ) -> None:
        async with self._lock:
            self._ensure_user_auto_locked(user_id)
            stats = self._execute(
                "SELECT sent_total FROM user_auto_stats WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            sent_total = (stats["sent_total"] if stats else 0) + sent
            self._execute(
                """
                INSERT INTO user_auto_stats (user_id, sent_total, last_sent_at, last_error)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    sent_total = excluded.sent_total,
                    last_sent_at = excluded.last_sent_at,
                    last_error = excluded.last_error
                """,
                (
                    user_id,
                    sent_total,
                    datetime.utcnow().isoformat() if sent else None,
                    "\n".join(errors) if errors else None,
                ),
            )
            if sent > 0:
                self._execute(
                    """
                    INSERT INTO auto_delivery_events (id, user_id, sent_count, delivered_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (uuid4().hex, user_id, int(sent), delivered_at or datetime.utcnow().isoformat()),
                )
            self._commit()

    async def list_known_chats(
        self,
        *,
        account_id: Optional[int] = None,
        owner_id: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            if account_id is not None:
                return self._list_user_account_chats_locked(account_id, owner_id=owner_id)
            return self._list_known_chats_locked()

    async def upsert_known_chat(self, chat_id: int, title: str, *, ensure_target: bool = False) -> None:
        async with self._lock:
            self._ensure_known_chat_locked(chat_id, title)
            self._commit()

    async def remove_known_chat(self, chat_id: int) -> None:
        async with self._lock:
            self._execute("DELETE FROM known_chats WHERE chat_id = ?", (chat_id,))
            self._execute("DELETE FROM user_auto_targets WHERE chat_id = ?", (chat_id,))
            self._commit()

    async def create_payment_request(
        self,
        *,
        user_id: int,
        username: Optional[str],
        full_name: str,
        card_number: str,
        card_name: str,
    ) -> str:
        async with self._lock:
            request_id = uuid4().hex
            created_at = datetime.utcnow().isoformat()
            self._execute(
                """
                INSERT INTO payments (
                    request_id, user_id, username, full_name,
                    card_number, card_name, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (request_id, user_id, username, full_name, card_number, card_name, created_at),
            )
            self._commit()
            return request_id

    async def set_payment_status(
        self,
        request_id: str,
        *,
        status: str,
        admin_id: int,
        admin_username: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        async with self._lock:
            row = self._execute(
                "SELECT request_id FROM payments WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if not row:
                return None
            resolved_at = datetime.utcnow().isoformat()
            self._execute(
                """
                UPDATE payments
                SET status = ?,
                    resolved_at = ?,
                    resolved_by_admin_id = ?,
                    resolved_by_admin_username = ?
                WHERE request_id = ?
                """,
                (status, resolved_at, admin_id, admin_username, request_id),
            )
            self._commit()
            return self._fetch_payment_locked(request_id)

    async def get_payment(self, request_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return self._fetch_payment_locked(request_id)

    async def has_recent_payment(self, *, within_days: int) -> bool:
        async with self._lock:
            threshold = datetime.utcnow() - timedelta(days=max(0, within_days))
            cur = self._execute(
                """
                SELECT resolved_at FROM payments
                WHERE status = 'approved' AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not cur:
                return False
            try:
                resolved_dt = datetime.fromisoformat(cur["resolved_at"])
            except (TypeError, ValueError):
                return False
            return resolved_dt >= threshold

    async def has_recent_payment_for_user(self, user_id: int, *, within_days: int) -> bool:
        async with self._lock:
            threshold = datetime.utcnow() - timedelta(days=max(0, within_days))
            cur = self._execute(
                """
                SELECT resolved_at FROM payments
                WHERE status = 'approved'
                  AND user_id = ?
                  AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if not cur or not cur["resolved_at"]:
                return False
            try:
                resolved_dt = datetime.fromisoformat(cur["resolved_at"])
            except (TypeError, ValueError):
                return False
            return resolved_dt >= threshold

    async def latest_payment_timestamp(self) -> Optional[datetime]:
        async with self._lock:
            cur = self._execute(
                """
                SELECT resolved_at FROM payments
                WHERE status = 'approved' AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not cur or cur["resolved_at"] is None:
                return None
            try:
                return datetime.fromisoformat(cur["resolved_at"])
            except ValueError:
                return None

    async def latest_payment_timestamp_for_user(self, user_id: int) -> Optional[datetime]:
        async with self._lock:
            cur = self._execute(
                """
                SELECT resolved_at FROM payments
                WHERE status = 'approved'
                  AND user_id = ?
                  AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if not cur or cur["resolved_at"] is None:
                return None
            try:
                return datetime.fromisoformat(cur["resolved_at"])
            except ValueError:
                return None

    async def get_user_payments(self, user_id: int) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                "SELECT * FROM payments WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            ).fetchall()
            return [self._row_to_payment(row) for row in rows]

    async def get_latest_payment_for_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            row = self._execute(
                """
                SELECT * FROM payments
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            return self._row_to_payment(row) if row else None

    async def find_user_id_by_username(self, username: str) -> Optional[int]:
        async with self._lock:
            row = self._execute(
                """
                SELECT user_id FROM payments
                WHERE LOWER(username) = LOWER(?)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (username,),
            ).fetchone()
            return int(row["user_id"]) if row else None

    async def get_all_payments(self) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                "SELECT * FROM payments ORDER BY created_at DESC"
            ).fetchall()
            return [self._row_to_payment(row) for row in rows]

    async def set_user_role(self, user_id: int, role: str) -> None:
        async with self._lock:
            self._execute(
                """
                INSERT INTO sessions (user_id, role, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    role = excluded.role,
                    updated_at = excluded.updated_at
                """,
                (user_id, role, datetime.utcnow().isoformat()),
            )
            self._commit()

    async def get_user_role(self, user_id: int) -> Optional[str]:
        async with self._lock:
            row = self._execute(
                "SELECT role FROM sessions WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return row["role"] if row else None

    async def list_admin_user_ids(self) -> List[int]:
        async with self._lock:
            rows = self._execute(
                "SELECT user_id FROM sessions WHERE role = 'admin'"
            ).fetchall()
            return [int(row["user_id"]) for row in rows]

    async def list_auto_user_ids(self) -> List[int]:
        async with self._lock:
            rows = self._execute("SELECT user_id FROM user_auto_configs").fetchall()
            return [int(row["user_id"]) for row in rows]

    async def ensure_constraints(self, user_id: Optional[int] = None, *, require_targets: bool = True) -> None:
        async with self._lock:
            if user_id is None:
                rows = self._execute("SELECT user_id FROM user_auto_configs").fetchall()
                targets = [int(row["user_id"]) for row in rows]
            else:
                targets = [user_id]
            for uid in targets:
                self._ensure_user_auto_locked(uid)
                auto = self._get_auto_locked(uid)
                account_id = auto.get("sender_account_id")
                if account_id is not None and not self._account_exists_locked(account_id):
                    self._execute(
                        "UPDATE user_auto_configs SET sender_account_id = NULL WHERE user_id = ?",
                        (uid,),
                    )
                    account_id = None
                has_message = bool(auto["message"])
                has_interval = (auto["interval_minutes"] or 0) > 0
                needs_targets = require_targets and account_id is None
                has_targets = bool(auto["target_chat_ids"]) or not needs_targets
                if auto["is_enabled"] and not (has_message and has_interval and has_targets):
                    self._execute(
                        "UPDATE user_auto_configs SET is_enabled = 0 WHERE user_id = ?",
                        (uid,),
                    )
            self._commit()

    def _get_auto_locked(self, user_id: int) -> Dict[str, Any]:
        self._ensure_user_auto_locked(user_id)
        config = self._execute(
            """
            SELECT message, interval_minutes, is_enabled, sender_account_id
            FROM user_auto_configs
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        sender_account_id = config["sender_account_id"] if config else None
        stats = self._execute(
            """
            SELECT sent_total, last_sent_at, last_error
            FROM user_auto_stats
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        if sender_account_id is None:
            cursor = self._execute(
                """
                SELECT chat_id FROM user_auto_targets
                WHERE user_id = ?
                ORDER BY chat_id
                """,
                (user_id,),
            )
        else:
            cursor = self._execute(
                """
                SELECT chat_id FROM user_account_targets
                WHERE user_id = ? AND account_id = ?
                ORDER BY chat_id
                """,
                (user_id, sender_account_id),
            )
        targets = [row["chat_id"] for row in cursor]
        return {
            "user_id": user_id,
            "message": config["message"] if config else None,
            "interval_minutes": config["interval_minutes"] if config else 0,
            "target_chat_ids": targets,
            "is_enabled": bool(config["is_enabled"]) if config else False,
            "sender_account_id": sender_account_id,
            "stats": {
                "sent_total": stats["sent_total"] if stats else 0,
                "last_sent_at": stats["last_sent_at"] if stats else None,
                "last_error": stats["last_error"] if stats else None,
            },
        }

    def _list_all_auto_locked(self) -> Dict[str, Dict[str, Any]]:
        rows = self._execute("SELECT user_id FROM user_auto_configs").fetchall()
        return {
            str(row["user_id"]): self._get_auto_locked(int(row["user_id"]))
            for row in rows
        }

    def _ensure_user_auto_locked(self, user_id: int) -> None:
        changed = False
        exists = self._execute(
            "SELECT 1 FROM user_auto_configs WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not exists:
            legacy = self._execute(
                "SELECT message, interval_minutes FROM auto_config WHERE id = 1"
            ).fetchone()
            message = legacy["message"] if legacy else None
            interval = legacy["interval_minutes"] if legacy and (legacy["interval_minutes"] or 0) > 0 else 60
            self._execute(
                """
                INSERT INTO user_auto_configs (user_id, message, interval_minutes, is_enabled)
                VALUES (?, ?, ?, 0)
                """,
                (user_id, message, interval),
            )
            changed = True
            legacy_targets = [
                row["chat_id"] for row in self._execute("SELECT chat_id FROM auto_targets").fetchall()
            ]
            if legacy_targets:
                self._executemany(
                    """
                    INSERT INTO user_auto_targets (user_id, chat_id)
                    VALUES (?, ?)
                    ON CONFLICT(user_id, chat_id) DO NOTHING
                    """,
                    [(user_id, chat_id) for chat_id in legacy_targets],
                )
                changed = True
        stats_exists = self._execute(
            "SELECT 1 FROM user_auto_stats WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not stats_exists:
            self._execute(
                "INSERT INTO user_auto_stats (user_id, sent_total) VALUES (?, 0)",
                (user_id,),
            )
            changed = True
        if changed:
            self._commit()

    def _account_exists_locked(self, account_id: int) -> bool:
        row = self._execute(
            "SELECT 1 FROM user_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        return row is not None

    def _list_known_chats_locked(self) -> Dict[str, Dict[str, Any]]:
        rows = self._execute(
            "SELECT chat_id, title FROM known_chats ORDER BY LOWER(title)"
        ).fetchall()
        return {
            str(row["chat_id"]): {"chat_id": row["chat_id"], "title": row["title"]}
            for row in rows
        }

    def _list_user_account_chats_locked(
        self,
        account_id: int,
        *,
        owner_id: Optional[int] = None,
    ) -> Dict[str, Dict[str, Any]]:
        account = self._execute(
            "SELECT id, owner_user_id FROM user_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if not account:
            return {}
        if owner_id is not None and int(account["owner_user_id"]) != int(owner_id):
            return {}
        rows = self._execute(
            """
            SELECT chat_id, title
            FROM user_account_chats
            WHERE account_id = ?
            ORDER BY LOWER(title)
            """,
            (account_id,),
        ).fetchall()
        return {
            str(row["chat_id"]): {"chat_id": row["chat_id"], "title": row["title"]}
            for row in rows
        }

    def _list_payments_locked(self) -> Dict[str, Dict[str, Any]]:
        rows = self._execute("SELECT * FROM payments").fetchall()
        return {row["request_id"]: self._row_to_payment(row) for row in rows}

    def _list_sessions_locked(self) -> Dict[str, Dict[str, Any]]:
        rows = self._execute("SELECT user_id, role, updated_at FROM sessions").fetchall()
        return {
            str(row["user_id"]): {"role": row["role"], "updated_at": row["updated_at"]}
            for row in rows
        }

    def _ensure_known_chat_locked(self, chat_id: int, title: str) -> None:
        sanitized_title = title.strip() if title else f"Чат {chat_id}"
        self._execute(
            """
            INSERT INTO known_chats (chat_id, title)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
            """,
            (chat_id, sanitized_title),
        )

    def _init_db(self) -> None:
        if not self._is_postgres:
            self._execute("PRAGMA foreign_keys = ON")
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                message TEXT,
                interval_minutes INTEGER NOT NULL DEFAULT 60,
                is_enabled INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                sent_total INTEGER NOT NULL DEFAULT 0,
                last_sent_at TEXT,
                last_error TEXT
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS known_chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_targets (
                chat_id BIGINT PRIMARY KEY,
                FOREIGN KEY(chat_id) REFERENCES known_chats(chat_id) ON DELETE CASCADE
            )
            """
        )
        if self._is_postgres:
            self._execute(
                """
                CREATE TABLE IF NOT EXISTS user_accounts (
                    id BIGSERIAL PRIMARY KEY,
                    owner_user_id BIGINT NOT NULL,
                    phone TEXT NOT NULL,
                    session TEXT NOT NULL,
                    title TEXT,
                    username TEXT,
                    last_synced_at TEXT,
                    proxy_type TEXT,
                    proxy_host TEXT,
                    proxy_port INTEGER,
                    proxy_username TEXT,
                    proxy_password TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        else:
            self._execute(
                """
                CREATE TABLE IF NOT EXISTS user_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_user_id BIGINT NOT NULL,
                    phone TEXT NOT NULL,
                    session TEXT NOT NULL,
                    title TEXT,
                    username TEXT,
                    last_synced_at TEXT,
                    proxy_type TEXT,
                    proxy_host TEXT,
                    proxy_port INTEGER,
                    proxy_username TEXT,
                    proxy_password TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        proxy_columns = {
            "proxy_type": "TEXT",
            "proxy_host": "TEXT",
            "proxy_port": "INTEGER",
            "proxy_username": "TEXT",
            "proxy_password": "TEXT",
        }
        for column, definition in proxy_columns.items():
            self._add_column_if_missing("user_accounts", column, definition)
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS user_account_chats (
                account_id INTEGER NOT NULL,
                chat_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                PRIMARY KEY(account_id, chat_id),
                FOREIGN KEY(account_id) REFERENCES user_accounts(id) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_accounts_owner
            ON user_accounts(owner_user_id)
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_account_chats_account
            ON user_account_chats(account_id)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS user_auto_configs (
                user_id BIGINT PRIMARY KEY,
                message TEXT,
                interval_minutes INTEGER NOT NULL DEFAULT 60,
                is_enabled INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._add_column_if_missing(
            "user_auto_configs",
            "sender_account_id",
            "INTEGER REFERENCES user_accounts(id) ON DELETE SET NULL",
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS user_auto_stats (
                user_id BIGINT PRIMARY KEY,
                sent_total INTEGER NOT NULL DEFAULT 0,
                last_sent_at TEXT,
                last_error TEXT
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_daily_limits (
                user_id BIGINT NOT NULL,
                day_key TEXT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(user_id, day_key)
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_campaign_events (
                id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                started_at TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_auto_campaign_events_started
            ON auto_campaign_events(started_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_delivery_events (
                id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0,
                delivered_at TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_auto_delivery_events_delivered
            ON auto_delivery_events(delivered_at)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS auto_chat_rate_limits (
                chat_id BIGINT PRIMARY KEY,
                last_sent_at TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS user_auto_targets (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                PRIMARY KEY(user_id, chat_id),
                FOREIGN KEY(chat_id) REFERENCES known_chats(chat_id) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS user_account_targets (
                user_id BIGINT NOT NULL,
                account_id INTEGER NOT NULL,
                chat_id BIGINT NOT NULL,
                PRIMARY KEY(user_id, account_id, chat_id),
                FOREIGN KEY(account_id) REFERENCES user_accounts(id) ON DELETE CASCADE
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_account_targets_account
            ON user_account_targets(account_id)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                request_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                full_name TEXT,
                card_number TEXT,
                card_name TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved_by_admin_id BIGINT,
                resolved_by_admin_username TEXT
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS audience_dumps (
                id TEXT PRIMARY KEY,
                owner_user_id BIGINT NOT NULL,
                source TEXT NOT NULL,
                file_path TEXT NOT NULL,
                total_users INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS invite_jobs (
                id TEXT PRIMARY KEY,
                owner_user_id BIGINT NOT NULL,
                target_chat TEXT NOT NULL,
                usernames_file TEXT NOT NULL,
                status TEXT NOT NULL,
                total_users INTEGER NOT NULL DEFAULT 0,
                invited_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                settings_json TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        self._execute(
            """
            CREATE INDEX IF NOT EXISTS idx_invite_jobs_owner
            ON invite_jobs(owner_user_id)
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                user_id BIGINT PRIMARY KEY,
                role TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._execute(
            """
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self._execute(
            """
            INSERT INTO auto_config (id, interval_minutes, is_enabled)
            VALUES (1, 60, 0)
            ON CONFLICT (id) DO NOTHING
            """
        )
        self._execute(
            """
            INSERT INTO auto_stats (id, sent_total)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
            """
        )
        self._commit()

    async def get_system_setting(self, key: str) -> Optional[str]:
        async with self._lock:
            row = self._execute("SELECT value FROM system_settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    async def set_system_setting(self, key: str, value: Optional[str]) -> None:
        async with self._lock:
            if value is None:
                self._execute("DELETE FROM system_settings WHERE key = ?", (key,))
            else:
                self._execute(
                    """
                    INSERT INTO system_settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, value),
                )
            self._commit()

    async def get_shared_proxy(self) -> Optional[Dict[str, Any]]:
        raw = await self.get_system_setting("shared_proxy")
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    async def set_shared_proxy(self, proxy: Optional[Dict[str, Any]]) -> None:
        value = json.dumps(proxy) if proxy else None
        await self.set_system_setting("shared_proxy", value)

    def _has_any_data(self) -> bool:
        cur = self._execute("SELECT message, is_enabled FROM auto_config WHERE id = 1").fetchone()
        if cur and (cur["message"] or cur["is_enabled"]):
            return True
        for table in ("known_chats", "auto_targets", "user_auto_configs", "user_auto_targets", "payments", "sessions"):
            row = self._execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
            if row and row["cnt"]:
                return True
        return False

    def _migrate_from_json(self, legacy_path: Path) -> None:
        try:
            raw = legacy_path.read_text(encoding="utf-8")
        except OSError:
            return
        if not raw.strip():
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        auto = data.get("auto") or {}
        known = data.get("known_chats") or {}
        payments = data.get("payments") or {}
        sessions = data.get("sessions") or {}
        self._execute(
            "UPDATE auto_config SET message = ?, interval_minutes = ?, is_enabled = ? WHERE id = 1",
            (
                auto.get("message"),
                auto.get("interval_minutes") or 0,
                1 if auto.get("is_enabled") else 0,
            ),
        )
        stats = auto.get("stats") or {}
        self._execute(
            "UPDATE auto_stats SET sent_total = ?, last_sent_at = ?, last_error = ? WHERE id = 1",
            (
                stats.get("sent_total", 0),
                stats.get("last_sent_at"),
                stats.get("last_error"),
            ),
        )
        targets: List[int] = list(auto.get("target_chat_ids") or [])
        self._execute("DELETE FROM auto_targets")
        if targets:
            self._executemany(
                """
                INSERT INTO auto_targets (chat_id)
                VALUES (?)
                ON CONFLICT (chat_id) DO NOTHING
                """,
                [(chat_id,) for chat_id in targets],
            )
        self._execute("DELETE FROM known_chats")
        if known:
            self._executemany(
                """
                INSERT INTO known_chats (chat_id, title)
                VALUES (?, ?)
                ON CONFLICT (chat_id) DO UPDATE SET title = excluded.title
                """,
                [
                    (
                        int(chat_id),
                        (info or {}).get("title") or f"Чат {chat_id}",
                    )
                    for chat_id, info in known.items()
                ],
            )
        self._execute("DELETE FROM payments")
        if payments:
            self._executemany(
                """
                INSERT INTO payments (
                    request_id, user_id, username, full_name,
                    card_number, card_name, status, created_at,
                    resolved_at, resolved_by_admin_id, resolved_by_admin_username
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        req_id,
                        (info or {}).get("user_id"),
                        (info or {}).get("username"),
                        (info or {}).get("full_name"),
                        (info or {}).get("card_number"),
                        (info or {}).get("card_name"),
                        (info or {}).get("status", "pending"),
                        (info or {}).get("created_at"),
                        (info or {}).get("resolved_at"),
                        ((info or {}).get("resolved_by") or {}).get("admin_id"),
                        ((info or {}).get("resolved_by") or {}).get("admin_username"),
                    )
                    for req_id, info in payments.items()
                ],
            )
        self._execute("DELETE FROM sessions")
        if sessions:
            self._executemany(
                """
                INSERT INTO sessions (user_id, role, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                    role = excluded.role,
                    updated_at = excluded.updated_at
                """,
                [
                    (
                        int(user_id),
                        (info or {}).get("role") or "user",
                        (info or {}).get("updated_at") or datetime.utcnow().isoformat(),
                    )
                    for user_id, info in sessions.items()
                ],
            )
        self._commit()

    def _fetch_payment_locked(self, request_id: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            "SELECT * FROM payments WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        return self._row_to_payment(row) if row else None

    def _fetch_audience_dump_locked(self, dump_id: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            """
            SELECT id, owner_user_id, source, file_path, total_users, created_at
            FROM audience_dumps
            WHERE id = ?
            """,
            (dump_id,),
        ).fetchone()
        return self._row_to_audience_dump(row) if row else None

    def _fetch_invite_job_locked(self, job_id: str) -> Optional[Dict[str, Any]]:
        row = self._execute(
            "SELECT * FROM invite_jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        return self._row_to_invite_job(row) if row else None

    def _row_to_payment(self, row: Any) -> Dict[str, Any]:
        data = dict(row)
        data["resolved_by"] = {
            "admin_id": data.pop("resolved_by_admin_id"),
            "admin_username": data.pop("resolved_by_admin_username"),
        }
        return data

    def _row_to_account(self, row: Any) -> Dict[str, Any]:
        data = dict(row)
        data["owner_user_id"] = int(data["owner_user_id"])
        if data.get("proxy_port") is not None:
            try:
                data["proxy_port"] = int(data["proxy_port"])
            except (TypeError, ValueError):
                data["proxy_port"] = None
        return data

    def _row_to_audience_dump(self, row: Any) -> Dict[str, Any]:
        data = dict(row)
        data["owner_user_id"] = int(data["owner_user_id"])
        data["total_users"] = int(data["total_users"] or 0)
        return data

    def _row_to_invite_job(self, row: Any) -> Dict[str, Any]:
        data = dict(row)
        data["owner_user_id"] = int(data["owner_user_id"])
        data["total_users"] = int(data["total_users"] or 0)
        data["invited_count"] = int(data["invited_count"] or 0)
        data["failed_count"] = int(data["failed_count"] or 0)
        settings_raw = data.pop("settings_json", None)
        try:
            data["settings"] = json.loads(settings_raw) if settings_raw else {}
        except json.JSONDecodeError:
            data["settings"] = {}
        return data
