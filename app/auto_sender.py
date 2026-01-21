import asyncio
from typing import List, Optional

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
            targets: List[int] = list(auto.get("target_chat_ids") or [])
            if not message or not targets or interval <= 0:
                await self._storage.ensure_constraints()
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
        if not auto.get("message") or not auto.get("target_chat_ids") or (auto.get("interval_minutes") or 0) <= 0:
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
