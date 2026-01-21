import asyncio
import logging
from typing import Dict, List, Optional

from aiogram import Bot
from aiogram.utils.exceptions import BotKicked, ChatNotFound, Unauthorized

from .storage import Storage
from .user_sender import UserSender


class AutoSender:
    def __init__(
        self,
        bot: Bot,
        storage: Storage,
        payment_valid_days: int,
        *,
        user_sender: Optional[UserSender] = None,
    ) -> None:
        self._bot = bot
        self._storage = storage
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._payment_valid_days = max(0, payment_valid_days)
        self._user_sender = user_sender
        self._personal_chats: Dict[int, str] = {}
        self._logger = logging.getLogger(__name__)

    async def start_if_enabled(self) -> None:
        auto = await self._storage.get_auto()
        if auto.get("is_enabled"):
            if not await self._payments_ready():
                return
            await self._ensure_constraints(auto)
            if auto.get("is_enabled"):
                await self._start_background()

    async def ensure_running(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="auto-sender")

    async def stop(self) -> None:
        async with self._lock:
            if not self._task:
                return
            self._stop_event.set()
            task = self._task
        await task
        async with self._lock:
            self._task = None
            self._stop_event.clear()

    async def refresh(self) -> None:
        auto = await self._storage.get_auto()
        if auto.get("is_enabled"):
            if not await self._payments_ready():
                await self.stop()
                return
            await self._ensure_constraints(auto)
            if auto.get("is_enabled"):
                await self.ensure_running()
                return
        await self.stop()

    async def _run(self) -> None:
        while True:
            auto = await self._storage.get_auto()
            if not auto.get("is_enabled"):
                break
            if not await self._payments_ready():
                break
            message = auto.get("message")
            interval = int(auto.get("interval_minutes") or 0)
            targets: List[int]
            if self._user_sender:
                personal_chats = await self.get_personal_chats(refresh=True)
                targets = list(personal_chats.keys())
            else:
                targets = list(auto.get("target_chat_ids") or [])
            if not message or not targets or interval <= 0:
                await self._storage.set_auto_enabled(False)
                break

            success = 0
            errors: List[str] = []
            for chat_id in targets:
                try:
                    if self._user_sender:
                        await self._user_sender.send_message(chat_id, message)
                    else:
                        await self._bot.send_message(chat_id, message)
                    success += 1
                except (BotKicked, ChatNotFound, Unauthorized) as exc:
                    errors.append(f"Недоступен чат {chat_id}: {exc}")
                except Exception as exc:  # pragma: no cover - network errors
                    errors.append(f"Ошибка доставки в чат {chat_id}: {exc}")
            await self._storage.update_stats(sent=success, errors=errors)

            wait_for = max(1, interval * 60)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait_for)
                self._stop_event.clear()
                break
            except asyncio.TimeoutError:
                continue
        async with self._lock:
            self._task = None
            self._stop_event.clear()

    async def _ensure_constraints(self, auto: dict) -> None:
        has_message = bool(auto.get("message"))
        interval_ok = (auto.get("interval_minutes") or 0) > 0
        has_targets = await self._has_target_chats(auto)
        if not (has_message and interval_ok and has_targets):
            await self._storage.set_auto_enabled(False)

    async def _start_background(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="auto-sender")

    async def _payments_ready(self) -> bool:
        if await self._storage.has_recent_payment(within_days=self._payment_valid_days):
            return True
        await self._storage.set_auto_enabled(False)
        return False

    async def get_personal_chats(self, *, refresh: bool = False) -> Dict[int, str]:
        if not self._user_sender:
            return {}
        if refresh or not self._personal_chats:
            await self._refresh_personal_chats()
        return dict(self._personal_chats)

    async def _refresh_personal_chats(self) -> None:
        if not self._user_sender:
            self._personal_chats = {}
            return
        try:
            dialogs = await self._user_sender.list_accessible_chats()
        except Exception:
            self._logger.exception("Не удалось получить список групп личного аккаунта.")
            return
        personal = {chat_id: title for chat_id, title in dialogs}
        existing = await self._storage.list_known_chats()
        existing_ids = {int(chat_id) for chat_id in existing.keys()}
        current_ids = set(personal.keys())
        for chat_id, title in personal.items():
            await self._storage.upsert_known_chat(chat_id, title)
        for stale_id in existing_ids - current_ids:
            await self._storage.remove_known_chat(stale_id)
        self._personal_chats = personal

    async def _has_target_chats(self, auto: dict) -> bool:
        targets = auto.get("target_chat_ids") or []
        if targets:
            return True
        if not self._user_sender:
            return False
        chats = await self.get_personal_chats(refresh=not self._personal_chats)
        return bool(chats)
