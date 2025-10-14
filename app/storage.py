import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

class Storage:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Any] = {
            "auto": {
                "message": None,
                "interval_minutes": 60,
                "target_chat_ids": [],
                "is_enabled": False,
                "stats": {
                    "sent_total": 0,
                    "last_sent_at": None,
                    "last_error": None,
                },
            },
            "known_chats": {},
            "payments": {},
            "sessions": {},
        }
        if self._path.exists():
            self._load_sync()
        else:
            self._write_sync()

    async def get_data(self) -> Dict[str, Any]:
        async with self._lock:
            return deepcopy(self._data)

    async def get_auto(self) -> Dict[str, Any]:
        async with self._lock:
            return deepcopy(self._data["auto"])

    async def set_auto_message(self, message: str) -> None:
        async with self._lock:
            self._data["auto"]["message"] = message
            await self._persist_locked()

    async def set_auto_interval(self, minutes: int) -> None:
        async with self._lock:
            self._data["auto"]["interval_minutes"] = minutes
            await self._persist_locked()

    async def set_auto_enabled(self, enabled: bool) -> None:
        async with self._lock:
            self._data["auto"]["is_enabled"] = enabled
            await self._persist_locked()

    async def toggle_target_chat(self, chat_id: int, title: Optional[str] = None) -> bool:
        async with self._lock:
            targets: List[int] = list(self._data["auto"]["target_chat_ids"])
            if chat_id in targets:
                targets.remove(chat_id)
                self._data["auto"]["target_chat_ids"] = targets
                await self._persist_locked()
                return False
            targets.append(chat_id)
            self._data["auto"]["target_chat_ids"] = targets
            if title:
                self._ensure_known_chat_locked(chat_id, title)
            await self._persist_locked()
            return True

    async def update_stats(self, *, sent: int, errors: List[str]) -> None:
        async with self._lock:
            stats = self._data["auto"]["stats"]
            stats["sent_total"] = stats.get("sent_total", 0) + sent
            stats["last_sent_at"] = datetime.utcnow().isoformat()
            stats["last_error"] = "\n".join(errors) if errors else None
            await self._persist_locked()

    async def list_known_chats(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return deepcopy(self._data["known_chats"])

    async def upsert_known_chat(self, chat_id: int, title: str, *, ensure_target: bool = False) -> None:
        async with self._lock:
            self._ensure_known_chat_locked(chat_id, title)
            if ensure_target and chat_id not in self._data["auto"]["target_chat_ids"]:
                self._data["auto"]["target_chat_ids"].append(chat_id)
            await self._persist_locked()

    async def remove_known_chat(self, chat_id: int) -> None:
        async with self._lock:
            self._data["known_chats"].pop(str(chat_id), None)
            targets = self._data["auto"]["target_chat_ids"]
            if chat_id in targets:
                targets.remove(chat_id)
            await self._persist_locked()

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
            self._data["payments"][request_id] = {
                "request_id": request_id,
                "user_id": user_id,
                "username": username,
                "full_name": full_name,
                "card_number": card_number,
                "card_name": card_name,
                "status": "pending",
                "created_at": created_at,
                "resolved_at": None,
                "resolved_by": None,
            }
            await self._persist_locked()
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
            payment = self._data["payments"].get(request_id)
            if not payment:
                return None
            payment = deepcopy(payment)
            self._data["payments"][request_id]["status"] = status
            self._data["payments"][request_id]["resolved_at"] = datetime.utcnow().isoformat()
            self._data["payments"][request_id]["resolved_by"] = {
                "admin_id": admin_id,
                "admin_username": admin_username,
            }
            await self._persist_locked()
            payment.update(self._data["payments"][request_id])
            return payment

    async def get_payment(self, request_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            payment = self._data["payments"].get(request_id)
            return deepcopy(payment) if payment else None

    async def has_recent_payment(self, *, within_days: int) -> bool:
        async with self._lock:
            payments = self._data.get("payments", {})
            if not payments:
                return False
            threshold = datetime.utcnow() - timedelta(days=max(0, within_days))
            for payment in payments.values():
                if (payment or {}).get("status") != "approved":
                    continue
                resolved_at = (payment or {}).get("resolved_at")
                if not resolved_at:
                    continue
                try:
                    resolved_dt = datetime.fromisoformat(resolved_at)
                except ValueError:
                    continue
                if resolved_dt >= threshold:
                    return True
            return False

    async def latest_payment_timestamp(self) -> Optional[datetime]:
        async with self._lock:
            payments = self._data.get("payments", {})
            latest: Optional[datetime] = None
            for payment in payments.values():
                if (payment or {}).get("status") != "approved":
                    continue
                resolved_at = (payment or {}).get("resolved_at")
                if not resolved_at:
                    continue
                try:
                    resolved_dt = datetime.fromisoformat(resolved_at)
                except ValueError:
                    continue
                if latest is None or resolved_dt > latest:
                    latest = resolved_dt
            return latest

    async def set_user_role(self, user_id: int, role: str) -> None:
        async with self._lock:
            self._data["sessions"][str(user_id)] = {
                "role": role,
                "updated_at": datetime.utcnow().isoformat(),
            }
            await self._persist_locked()

    async def get_user_role(self, user_id: int) -> Optional[str]:
        async with self._lock:
            session = self._data["sessions"].get(str(user_id))
            if not session:
                return None
            return session.get("role")

    async def list_admin_user_ids(self) -> List[int]:
        async with self._lock:
            admin_ids = []
            for key, session in self._data["sessions"].items():
                if (session or {}).get("role") == "admin":
                    try:
                        admin_ids.append(int(key))
                    except ValueError:
                        continue
            return admin_ids

    async def ensure_constraints(self) -> None:
        """Disable autoresend if config is incomplete."""
        async with self._lock:
            auto = self._data["auto"]
            if not auto["message"] or not auto["target_chat_ids"] or auto["interval_minutes"] <= 0:
                auto["is_enabled"] = False
                await self._persist_locked()

    def _ensure_known_chat_locked(self, chat_id: int, title: str) -> None:
        key = str(chat_id)
        sanitized_title = title.strip() if title else f"Чат {chat_id}"
        self._data["known_chats"][key] = {
            "chat_id": chat_id,
            "title": sanitized_title,
        }

    async def _persist_locked(self) -> None:
        data = deepcopy(self._data)
        await asyncio.to_thread(self._write_sync_data, data)

    def _load_sync(self) -> None:
        raw = self._path.read_text(encoding="utf-8")
        if not raw.strip():
            self._write_sync()
            return
        loaded = json.loads(raw)
        self._data.update(loaded)
        self._data.setdefault("payments", {})
        self._data.setdefault("sessions", {})

    def _write_sync(self) -> None:
        self._path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_sync_data(self, data: Dict[str, Any]) -> None:
        self._path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
