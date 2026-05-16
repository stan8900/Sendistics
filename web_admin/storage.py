import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - PostgreSQL driver is optional for local SQLite
    psycopg = None
    dict_row = None


class AnalyticsStorage:
    """Read-only database access used by the standalone web dashboard."""

    def __init__(self, path: Optional[Path], *, database_url: Optional[str] = None) -> None:
        self._path = path
        self._database_url = database_url
        self._is_postgres = bool(database_url)
        self._lock = asyncio.Lock()
        if self._is_postgres:
            if psycopg is None:
                raise RuntimeError("psycopg is required for PostgreSQL storage. Install psycopg[binary].")
            self._conn = psycopg.connect(database_url, autocommit=True, row_factory=dict_row)
        else:
            if path is None:
                raise ValueError("Storage path is required when DATABASE_URL is not set.")
            path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row

    def _prepare_query(self, query: str) -> str:
        if not self._is_postgres:
            return query
        return query.replace("?", "%s")

    def _execute(self, query: str, params: Sequence[Any] = ()) -> Any:
        return self._conn.execute(self._prepare_query(query), params)

    @staticmethod
    def _row_to_dict(row: Any) -> Dict[str, Any]:
        return dict(row)

    async def get_all_payments(self) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._execute(
                """
                SELECT request_id, user_id, username, full_name, card_number, card_name,
                       status, created_at, resolved_at, resolved_by_admin_id,
                       resolved_by_admin_username
                FROM payments
                ORDER BY created_at DESC
                """
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

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
            return [self._row_to_dict(row) for row in rows]

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
            return [self._row_to_dict(row) for row in rows]

    async def count_active_auto_campaigns(self) -> int:
        async with self._lock:
            row = self._execute(
                "SELECT COUNT(*) AS cnt FROM user_auto_configs WHERE is_enabled = 1"
            ).fetchone()
            return int(row["cnt"] or 0) if row else 0

    async def latest_payment_timestamp(self) -> Optional[datetime]:
        async with self._lock:
            row = self._execute(
                """
                SELECT resolved_at FROM payments
                WHERE status = 'approved' AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 1
                """
            ).fetchone()
            if not row or row["resolved_at"] is None:
                return None
            try:
                return datetime.fromisoformat(row["resolved_at"])
            except ValueError:
                return None
