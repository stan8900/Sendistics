import asyncio
import hashlib
import hmac
import json
import secrets
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional


def _default_auto_config() -> Dict[str, Any]:
    return {
        "message": None,
        "interval_minutes": 60,
        "target_chat_ids": [],
        "is_enabled": False,
        "stats": {
            "sent_total": 0,
            "last_sent_at": None,
            "last_error": None,
        },
    }


class Storage:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Any] = {
            "auto": _default_auto_config(),
            "known_chats": {},
            "accounts": {},
            "sessions": {},
            "account_auto": {},
            "pending_phone_logins": {},
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

    async def get_account_auto(self, account_id: str) -> Dict[str, Any]:
        async with self._lock:
            return deepcopy(self._get_account_auto_locked(account_id))

    async def set_auto_message(self, message: str) -> None:
        async with self._lock:
            self._data["auto"]["message"] = message
            await self._persist_locked()

    async def set_account_auto_message(self, account_id: str, message: str) -> None:
        async with self._lock:
            self._get_account_auto_locked(account_id)["message"] = message
            await self._persist_locked()

    async def set_auto_interval(self, minutes: int) -> None:
        async with self._lock:
            self._data["auto"]["interval_minutes"] = minutes
            await self._persist_locked()

    async def set_account_auto_interval(self, account_id: str, minutes: int) -> None:
        async with self._lock:
            self._get_account_auto_locked(account_id)["interval_minutes"] = minutes
            await self._persist_locked()

    async def set_auto_enabled(self, enabled: bool) -> None:
        async with self._lock:
            self._data["auto"]["is_enabled"] = enabled
            await self._persist_locked()

    async def set_account_auto_enabled(self, account_id: str, enabled: bool) -> None:
        async with self._lock:
            self._get_account_auto_locked(account_id)["is_enabled"] = enabled
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

    async def toggle_account_target_chat(self, account_id: str, chat_id: int, title: Optional[str] = None) -> bool:
        async with self._lock:
            auto = self._get_account_auto_locked(account_id)
            targets: List[int] = list(auto["target_chat_ids"])
            if chat_id in targets:
                targets.remove(chat_id)
                auto["target_chat_ids"] = targets
                await self._persist_locked()
                return False
            targets.append(chat_id)
            auto["target_chat_ids"] = targets
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

    async def update_account_stats(self, account_id: str, *, sent: int, errors: List[str]) -> None:
        async with self._lock:
            stats = self._get_account_auto_locked(account_id)["stats"]
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

    async def ensure_constraints(self) -> None:
        """Disable autoresend if config is incomplete."""
        async with self._lock:
            auto = self._data["auto"]
            if not auto["message"] or not auto["target_chat_ids"] or auto["interval_minutes"] <= 0:
                auto["is_enabled"] = False
                await self._persist_locked()

    async def ensure_account_constraints(self, account_id: str) -> None:
        async with self._lock:
            auto = self._get_account_auto_locked(account_id)
            if not auto["message"] or not auto["target_chat_ids"] or auto["interval_minutes"] <= 0:
                auto["is_enabled"] = False
                await self._persist_locked()

    async def create_account(
        self,
        username: str,
        password: str,
        *,
        telegram_user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        username = self._normalize_username(username)
        self._validate_password(password)
        async with self._lock:
            if self._find_account_by_username_locked(username):
                raise ValueError("Аккаунт с таким именем уже существует")

            account_id = secrets.token_urlsafe(16)
            salt = secrets.token_hex(16)
            now = datetime.utcnow().isoformat()
            account = {
                "id": account_id,
                "username": username,
                "password_hash": self._hash_password(password, salt),
                "salt": salt,
                "created_at": now,
                "telegram_user_ids": [],
                "payments": [],
                "phone_numbers": [],
            }
            self._data["accounts"][account_id] = account
            self._data["account_auto"][account_id] = _default_auto_config()
            if telegram_user_id is not None:
                self._link_telegram_user_locked(account_id, telegram_user_id)
            await self._persist_locked()
            return self._public_account(account)

    async def authenticate_account(
        self,
        username: str,
        password: str,
        *,
        telegram_user_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        username = self._normalize_username(username)
        async with self._lock:
            account = self._find_account_by_username_locked(username)
            if not account:
                return None
            if "password_hash" not in account or "salt" not in account:
                return None
            expected = account["password_hash"]
            actual = self._hash_password(password, account["salt"])
            if not hmac.compare_digest(expected, actual):
                return None
            if telegram_user_id is not None:
                self._link_telegram_user_locked(account["id"], telegram_user_id)
                await self._persist_locked()
            return self._public_account(account)

    async def logout_telegram_user(self, telegram_user_id: int) -> None:
        async with self._lock:
            self._data["sessions"].pop(str(telegram_user_id), None)
            await self._persist_locked()

    async def get_active_account(self, telegram_user_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            account_id = self._data["sessions"].get(str(telegram_user_id))
            account = self._data["accounts"].get(account_id) if account_id else None
            return self._public_account(account) if account else None

    async def require_active_account(self, telegram_user_id: int) -> Dict[str, Any]:
        account = await self.get_active_account(telegram_user_id)
        if not account:
            raise PermissionError("Нужно войти в личный аккаунт")
        return account

    async def start_phone_login(self, phone_number: str) -> Dict[str, str]:
        phone_number = self._normalize_phone_number(phone_number)
        code = f"{secrets.randbelow(1_000_000):06d}"
        salt = secrets.token_hex(16)
        expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
        async with self._lock:
            self._data["pending_phone_logins"][phone_number] = {
                "code_hash": self._hash_password(code, salt),
                "salt": salt,
                "expires_at": expires_at,
                "attempts": 0,
            }
            await self._persist_locked()
            return {
                "phone_number": phone_number,
                "code": code,
                "expires_at": expires_at,
            }

    async def verify_phone_login(
        self,
        phone_number: str,
        code: str,
        *,
        telegram_user_id: int,
    ) -> Optional[Dict[str, Any]]:
        phone_number = self._normalize_phone_number(phone_number)
        clean_code = (code or "").strip()
        async with self._lock:
            pending = self._data["pending_phone_logins"].get(phone_number)
            if not pending:
                return None
            if datetime.utcnow() > datetime.fromisoformat(pending["expires_at"]):
                self._data["pending_phone_logins"].pop(phone_number, None)
                await self._persist_locked()
                return None
            pending["attempts"] = pending.get("attempts", 0) + 1
            expected = pending["code_hash"]
            actual = self._hash_password(clean_code, pending["salt"])
            if pending["attempts"] > 5 or not hmac.compare_digest(expected, actual):
                if pending["attempts"] > 5:
                    self._data["pending_phone_logins"].pop(phone_number, None)
                await self._persist_locked()
                return None

            account = self._find_account_by_phone_locked(phone_number)
            if not account:
                account = self._create_phone_account_locked(phone_number)
            self._link_telegram_user_locked(account["id"], telegram_user_id)
            self._data["pending_phone_logins"].pop(phone_number, None)
            await self._persist_locked()
            return self._public_account(account)

    async def login_with_verified_phone(self, phone_number: str, *, telegram_user_id: int) -> Dict[str, Any]:
        phone_number = self._normalize_phone_number(phone_number)
        async with self._lock:
            account = self._find_account_by_phone_locked(phone_number)
            if not account:
                account = self._create_phone_account_locked(phone_number)
            self._link_telegram_user_locked(account["id"], telegram_user_id)
            await self._persist_locked()
            return self._public_account(account)

    async def add_account_payment(
        self,
        account_id: str,
        *,
        amount: float,
        currency: str,
        status: str,
        description: str = "",
    ) -> Dict[str, Any]:
        async with self._lock:
            account = self._data["accounts"][account_id]
            payment = {
                "id": secrets.token_urlsafe(12),
                "amount": amount,
                "currency": currency.upper(),
                "status": status,
                "description": description,
                "created_at": datetime.utcnow().isoformat(),
            }
            account.setdefault("payments", []).append(payment)
            await self._persist_locked()
            return deepcopy(payment)

    async def list_account_payments(self, account_id: str) -> List[Dict[str, Any]]:
        async with self._lock:
            account = self._data["accounts"][account_id]
            return deepcopy(account.setdefault("payments", []))

    async def add_account_phone_number(
        self,
        account_id: str,
        phone_number: str,
        *,
        label: str = "",
        auto_enabled: bool = False,
    ) -> Dict[str, Any]:
        phone_number = self._normalize_phone_number(phone_number)
        async with self._lock:
            account = self._data["accounts"][account_id]
            numbers = account.setdefault("phone_numbers", [])
            existing = next((item for item in numbers if item["phone_number"] == phone_number), None)
            if existing:
                existing["label"] = label or existing.get("label", "")
                existing["auto_enabled"] = auto_enabled
                await self._persist_locked()
                return deepcopy(existing)
            number = {
                "id": secrets.token_urlsafe(12),
                "phone_number": phone_number,
                "label": label,
                "auto_enabled": auto_enabled,
                "created_at": datetime.utcnow().isoformat(),
            }
            numbers.append(number)
            await self._persist_locked()
            return deepcopy(number)

    async def list_account_phone_numbers(self, account_id: str) -> List[Dict[str, Any]]:
        async with self._lock:
            account = self._data["accounts"][account_id]
            return deepcopy(account.setdefault("phone_numbers", []))

    async def get_account_dashboard(self, telegram_user_id: int) -> Optional[Dict[str, Any]]:
        async with self._lock:
            account_id = self._data["sessions"].get(str(telegram_user_id))
            account = self._data["accounts"].get(account_id) if account_id else None
            if not account:
                return None
            return {
                "account": self._public_account(account),
                "payments": deepcopy(account.setdefault("payments", [])),
                "phone_numbers": deepcopy(account.setdefault("phone_numbers", [])),
                "auto": deepcopy(self._get_account_auto_locked(account["id"])),
            }

    def _ensure_known_chat_locked(self, chat_id: int, title: str) -> None:
        key = str(chat_id)
        sanitized_title = title.strip() if title else f"Чат {chat_id}"
        self._data["known_chats"][key] = {
            "chat_id": chat_id,
            "title": sanitized_title,
        }

    def _get_account_auto_locked(self, account_id: str) -> Dict[str, Any]:
        if account_id not in self._data["accounts"]:
            raise KeyError(f"Unknown account: {account_id}")
        account_auto = self._data.setdefault("account_auto", {})
        if account_id not in account_auto:
            account_auto[account_id] = _default_auto_config()
        return account_auto[account_id]

    def _link_telegram_user_locked(self, account_id: str, telegram_user_id: int) -> None:
        account = self._data["accounts"][account_id]
        telegram_id = int(telegram_user_id)
        if telegram_id not in account["telegram_user_ids"]:
            account["telegram_user_ids"].append(telegram_id)
        self._data["sessions"][str(telegram_id)] = account_id

    def _create_phone_account_locked(self, phone_number: str) -> Dict[str, Any]:
        account_id = secrets.token_urlsafe(16)
        now = datetime.utcnow().isoformat()
        account = {
            "id": account_id,
            "username": phone_number,
            "phone_number": phone_number,
            "created_at": now,
            "telegram_user_ids": [],
            "payments": [],
            "phone_numbers": [
                {
                    "id": secrets.token_urlsafe(12),
                    "phone_number": phone_number,
                    "label": "Основной номер",
                    "auto_enabled": False,
                    "created_at": now,
                }
            ],
        }
        self._data["accounts"][account_id] = account
        self._data["account_auto"][account_id] = _default_auto_config()
        return account

    def _find_account_by_username_locked(self, username: str) -> Optional[Dict[str, Any]]:
        for account in self._data["accounts"].values():
            if account.get("username") == username:
                return account
        return None

    def _find_account_by_phone_locked(self, phone_number: str) -> Optional[Dict[str, Any]]:
        for account in self._data["accounts"].values():
            if account.get("phone_number") == phone_number:
                return account
        return None

    def _normalize_username(self, username: str) -> str:
        normalized = (username or "").strip().lower()
        if len(normalized) < 3:
            raise ValueError("Имя аккаунта должно быть минимум 3 символа")
        if len(normalized) > 32:
            raise ValueError("Имя аккаунта должно быть не длиннее 32 символов")
        allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_-.")
        if any(char not in allowed for char in normalized):
            raise ValueError("Используйте латиницу, цифры, точку, дефис или подчёркивание")
        return normalized

    def _validate_password(self, password: str) -> None:
        if len(password or "") < 8:
            raise ValueError("Пароль должен быть минимум 8 символов")

    def _normalize_phone_number(self, phone_number: str) -> str:
        raw = (phone_number or "").strip()
        if raw.startswith("+"):
            prefix = "+"
            digits = "".join(char for char in raw[1:] if char.isdigit())
        else:
            prefix = "+"
            digits = "".join(char for char in raw if char.isdigit())
        if len(digits) < 10 or len(digits) > 15:
            raise ValueError("Введите номер в международном формате, например +447700900123")
        return f"{prefix}{digits}"

    def _hash_password(self, password: str, salt: str) -> str:
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            200_000,
        )
        return digest.hex()

    def _public_account(self, account: Dict[str, Any]) -> Dict[str, Any]:
        public = deepcopy(account)
        public.pop("password_hash", None)
        public.pop("salt", None)
        return public

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
        self._data.setdefault("auto", _default_auto_config())
        self._data.setdefault("known_chats", {})
        self._data.setdefault("accounts", {})
        self._data.setdefault("sessions", {})
        self._data.setdefault("account_auto", {})
        self._data.setdefault("pending_phone_logins", {})
        for account_id in self._data["accounts"]:
            self._data["account_auto"].setdefault(account_id, _default_auto_config())
            account = self._data["accounts"][account_id]
            account.setdefault("payments", [])
            account.setdefault("phone_numbers", [])

    def _write_sync(self) -> None:
        self._path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _write_sync_data(self, data: Dict[str, Any]) -> None:
        self._path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
