import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set
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
            params_list = list(seq_of_params)
            if not params_list:
                return None
            with self._conn.cursor() as cur:
                cur.executemany(sql, params_list)
                return cur
        return self._conn.executemany(sql, seq_of_params)

    def _commit(self) -> None:
        if not self._is_postgres:
            self._conn.commit()

    async def get_data(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "auto": self._get_auto_locked(),
                "known_chats": self._list_known_chats_locked(),
                "payments": self._list_payments_locked(),
                "sessions": self._list_sessions_locked(),
            }

    async def get_auto(self) -> Dict[str, Any]:
        async with self._lock:
            return self._get_auto_locked()

    async def set_auto_message(self, message: str) -> None:
        async with self._lock:
            self._execute(
                "UPDATE auto_config SET message = ? WHERE id = 1",
                (message,),
            )
            self._commit()

    async def set_auto_interval(self, minutes: int) -> None:
        async with self._lock:
            self._execute(
                "UPDATE auto_config SET interval_minutes = ? WHERE id = 1",
                (minutes,),
            )
            self._commit()

    async def set_auto_enabled(self, enabled: bool) -> None:
        async with self._lock:
            self._execute(
                "UPDATE auto_config SET is_enabled = ? WHERE id = 1",
                (1 if enabled else 0,),
            )
            self._commit()

    async def toggle_target_chat(self, chat_id: int, title: Optional[str] = None) -> bool:
        async with self._lock:
            cur = self._execute("SELECT 1 FROM auto_targets WHERE chat_id = ?", (chat_id,))
            exists = cur.fetchone() is not None
            if exists:
                self._execute("DELETE FROM auto_targets WHERE chat_id = ?", (chat_id,))
                self._commit()
                return False
            self._execute(
                "INSERT INTO auto_targets (chat_id) VALUES (?) ON CONFLICT (chat_id) DO NOTHING",
                (chat_id,),
            )
            if title:
                self._ensure_known_chat_locked(chat_id, title)
            self._commit()
            return True

    async def update_stats(self, *, sent: int, errors: List[str]) -> None:
        async with self._lock:
            stats = self._execute("SELECT sent_total FROM auto_stats WHERE id = 1").fetchone()
            sent_total = (stats["sent_total"] if stats else 0) + sent
            self._execute(
                "UPDATE auto_stats SET sent_total = ?, last_sent_at = ?, last_error = ? WHERE id = 1",
                (
                    sent_total,
                    datetime.utcnow().isoformat(),
                    "\n".join(errors) if errors else None,
                ),
            )
            self._commit()

    async def list_known_chats(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return self._list_known_chats_locked()

    async def upsert_known_chat(
        self,
        chat_id: int,
        title: str,
        *,
        ensure_target: bool = False,
        delivery_available: Optional[bool] = None,
    ) -> None:
        async with self._lock:
            self._ensure_known_chat_locked(chat_id, title, delivery_available=delivery_available)
            if ensure_target:
                self._execute(
                    "INSERT INTO auto_targets (chat_id) VALUES (?) ON CONFLICT (chat_id) DO NOTHING",
                    (chat_id,),
                )
            self._commit()

    async def remove_known_chat(self, chat_id: int) -> None:
        async with self._lock:
            self._execute("DELETE FROM known_chats WHERE chat_id = ?", (chat_id,))
            self._execute("DELETE FROM auto_targets WHERE chat_id = ?", (chat_id,))
            self._commit()

    async def set_delivery_available(self, chat_id: int, available: bool) -> None:
        async with self._lock:
            self._execute(
                "UPDATE known_chats SET delivery_available = ? WHERE chat_id = ?",
                (1 if available else 0, chat_id),
            )
            self._commit()

    async def is_delivery_available(self, chat_id: int) -> bool:
        async with self._lock:
            row = self._execute(
                "SELECT delivery_available FROM known_chats WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
            return bool(row and row["delivery_available"])

    async def list_delivery_ready_chat_ids(self) -> Set[int]:
        async with self._lock:
            rows = self._execute(
                "SELECT chat_id FROM known_chats WHERE delivery_available = 1"
            ).fetchall()
            return {int(row["chat_id"]) for row in rows}

    async def set_target_chats(self, chat_ids: Iterable[int]) -> None:
        async with self._lock:
            unique_ids: List[int] = []
            seen = set()
            for chat_id in chat_ids:
                try:
                    cid = int(chat_id)
                except (TypeError, ValueError):
                    continue
                if cid in seen:
                    continue
                seen.add(cid)
                unique_ids.append(cid)
            self._execute("DELETE FROM auto_targets")
            if unique_ids:
                self._executemany(
                    "INSERT INTO auto_targets (chat_id) VALUES (?)",
                    ((chat_id,) for chat_id in unique_ids),
                )
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

    async def ensure_constraints(self) -> None:
        async with self._lock:
            auto = self._get_auto_locked()
            targets_valid = bool(auto["target_chat_ids"])
            if not auto["message"] or not targets_valid or auto["interval_minutes"] <= 0:
                self._execute("UPDATE auto_config SET is_enabled = 0 WHERE id = 1")
                self._commit()

    def _get_auto_locked(self) -> Dict[str, Any]:
        config = self._execute(
            "SELECT message, interval_minutes, is_enabled FROM auto_config WHERE id = 1"
        ).fetchone()
        stats = self._execute(
            "SELECT sent_total, last_sent_at, last_error FROM auto_stats WHERE id = 1"
        ).fetchone()
        targets = [
            row["chat_id"]
            for row in self._execute(
                "SELECT chat_id FROM auto_targets ORDER BY chat_id"
            )
        ]
        auto = {
            "message": config["message"] if config else None,
            "interval_minutes": config["interval_minutes"] if config else 0,
            "target_chat_ids": targets,
            "is_enabled": bool(config["is_enabled"]) if config else False,
            "stats": {
                "sent_total": stats["sent_total"] if stats else 0,
                "last_sent_at": stats["last_sent_at"] if stats else None,
                "last_error": stats["last_error"] if stats else None,
            },
        }
        return auto

    def _list_known_chats_locked(self) -> Dict[str, Dict[str, Any]]:
        rows = self._execute(
            "SELECT chat_id, title, delivery_available FROM known_chats ORDER BY LOWER(title)"
        ).fetchall()
        return {
            str(row["chat_id"]): {
                "chat_id": row["chat_id"],
                "title": row["title"],
                "delivery_available": bool(row["delivery_available"]),
            }
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

    def _ensure_known_chat_locked(
        self,
        chat_id: int,
        title: str,
        *,
        delivery_available: Optional[bool] = None,
    ) -> None:
        sanitized_title = title.strip() if title else f"Чат {chat_id}"
        if delivery_available is None:
            self._execute(
                """
                INSERT INTO known_chats (chat_id, title)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
                """,
                (chat_id, sanitized_title),
            )
            return
        self._execute(
            """
            INSERT INTO known_chats (chat_id, title, delivery_available)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                delivery_available = excluded.delivery_available
            """,
            (chat_id, sanitized_title, 1 if delivery_available else 0),
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
            CREATE TABLE IF NOT EXISTS sessions (
                user_id BIGINT PRIMARY KEY,
                role TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._ensure_known_chats_schema()

    def _ensure_known_chats_schema(self) -> None:
        if self._is_postgres:
            self._execute(
                """
                ALTER TABLE known_chats
                ADD COLUMN IF NOT EXISTS delivery_available BOOLEAN NOT NULL DEFAULT FALSE
                """
            )
            return
        try:
            self._execute(
                "ALTER TABLE known_chats ADD COLUMN delivery_available INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError as exc:  # column already exists
            message = str(exc).lower()
            if "duplicate column name" not in message and "already exists" not in message:
                raise
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

    def _has_any_data(self) -> bool:
        cur = self._execute("SELECT message, is_enabled FROM auto_config WHERE id = 1").fetchone()
        if cur and (cur["message"] or cur["is_enabled"]):
            return True
        for table in ("known_chats", "auto_targets", "payments", "sessions"):
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

    def _row_to_payment(self, row: Any) -> Dict[str, Any]:
        data = dict(row)
        data["resolved_by"] = {
            "admin_id": data.pop("resolved_by_admin_id"),
            "admin_username": data.pop("resolved_by_admin_username"),
        }
        return data
