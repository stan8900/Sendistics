import asyncio
from typing import List

from aiogram import Bot, types
from aiogram.utils.exceptions import BotKicked, ChatNotFound, Unauthorized

from .storage import Storage


class AutoSender:
    def __init__(self, bot: Bot, storage: Storage, bot_id: int) -> None:
        self._bot = bot
        self._storage = storage
        self._bot_id = bot_id
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def start_if_enabled(self) -> None:
        auto = await self._storage.get_auto()
        if auto.get("is_enabled"):
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
                    if not await self._is_admin(chat_id):
                        errors.append(f"Нет прав администратора в чате {chat_id}")
                        continue
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

    async def _is_admin(self, chat_id: int) -> bool:
        member: types.ChatMember = await self._bot.get_chat_member(chat_id, self._bot_id)
        return member.is_chat_admin()
