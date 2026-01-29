import asyncio
import logging
from typing import Awaitable, Callable, Dict, List, Optional

from aiogram.utils.exceptions import BotKicked, ChatNotFound, Unauthorized

from .storage import Storage


class AutoSender:
    def __init__(
        self,
        send_message: Callable[[int, str], Awaitable[None]],
        storage: Storage,
        payment_valid_days: int,
    ) -> None:
        self._send_message = send_message
        self._storage = storage
        self._payment_valid_days = max(0, payment_valid_days)
        self._logger = logging.getLogger(__name__)
        self._lock = asyncio.Lock()
        self._tasks: Dict[int, asyncio.Task[None]] = {}
        self._stop_events: Dict[int, asyncio.Event] = {}

    async def start_if_enabled(self) -> None:
        campaigns = await self._storage.list_active_campaigns()
        for campaign in campaigns:
            owner_id = int(campaign.get("owner_id"))
            await self._start_for_owner(owner_id)

    async def ensure_running(self, owner_id: int) -> None:
        async with self._lock:
            task = self._tasks.get(owner_id)
            if task and not task.done():
                return
            stop_event = asyncio.Event()
            self._stop_events[owner_id] = stop_event
            self._tasks[owner_id] = asyncio.create_task(
                self._run(owner_id, stop_event),
                name=f"auto-sender-{owner_id}",
            )

    async def stop(self, owner_id: Optional[int] = None) -> None:
        async with self._lock:
            if owner_id is None:
                targets = list(self._tasks.items())
            else:
                task = self._tasks.get(owner_id)
                targets = [(owner_id, task)] if task else []
            for oid, task in targets:
                if not task:
                    continue
                stop_event = self._stop_events.get(oid)
                if stop_event:
                    stop_event.set()
        for _, task in targets:
            if task:
                await task

    async def refresh(self, owner_id: Optional[int] = None) -> None:
        if owner_id is None:
            campaigns = await self._storage.list_auto_campaigns()
            for campaign in campaigns:
                await self._refresh_owner(int(campaign.get("owner_id")))
            return
        await self._refresh_owner(owner_id)

    async def _start_for_owner(self, owner_id: int) -> None:
        if not await self._prepare_campaign(owner_id):
            return
        await self.ensure_running(owner_id)

    async def _refresh_owner(self, owner_id: int) -> None:
        campaign = await self._storage.get_auto(owner_id)
        if not campaign.get("is_enabled"):
            await self.stop(owner_id)
            return
        if not await self._prepare_campaign(owner_id):
            await self.stop(owner_id)
            return
        await self.stop(owner_id)
        await self.ensure_running(owner_id)

    async def _prepare_campaign(self, owner_id: int) -> bool:
        campaign = await self._storage.get_auto(owner_id)
        if not campaign.get("is_enabled"):
            return False
        if not await self._payments_ready(owner_id):
            return False
        await self._storage.ensure_constraints(owner_id)
        updated = await self._storage.get_auto(owner_id)
        return bool(updated.get("is_enabled"))

    async def _run(self, owner_id: int, stop_event: asyncio.Event) -> None:
        try:
            while True:
                campaign = await self._storage.get_auto(owner_id)
                if not campaign.get("is_enabled"):
                    break
                if not await self._payments_ready(owner_id):
                    break
                message = campaign.get("message")
                interval = int(campaign.get("interval_minutes") or 0)
                targets: List[int] = list(campaign.get("target_chat_ids") or [])
                if targets:
                    delivery_ready = await self._storage.list_delivery_ready_chat_ids()
                    targets = [chat_id for chat_id in targets if chat_id in delivery_ready]
                if not message or not targets or interval <= 0:
                    await self._storage.set_auto_enabled(owner_id, False)
                    break
                success = 0
                errors: List[str] = []
                for chat_id in targets:
                    try:
                        await self._send_message(chat_id, message)
                        success += 1
                    except (BotKicked, ChatNotFound, Unauthorized) as exc:
                        errors.append(f"Недоступен чат {chat_id}: {exc}")
                    except Exception as exc:  # pragma: no cover - network errors
                        errors.append(f"Ошибка доставки в чат {chat_id}: {exc}")
                await self._storage.update_stats(owner_id, sent=success, errors=errors)
                wait_for = max(1, interval * 60)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_for)
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            async with self._lock:
                task = self._tasks.get(owner_id)
                current = asyncio.current_task()
                if task is current:
                    self._tasks.pop(owner_id, None)
                    self._stop_events.pop(owner_id, None)
                stop_event.clear()

    async def _payments_ready(self, owner_id: int) -> bool:
        user_paid = await self._storage.has_recent_payment_for_user(
            owner_id,
            within_days=self._payment_valid_days,
        )
        system_paid = await self._storage.has_recent_payment(within_days=self._payment_valid_days)
        if user_paid and system_paid:
            return True
        await self._storage.set_auto_enabled(owner_id, False)
        return False
