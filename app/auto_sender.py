import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.utils.exceptions import BotKicked, ChatNotFound, Unauthorized

from .account_manager import AccountManager
from .storage import Storage
from .user_sender import InvalidUserSessionError, UserSender


TASHKENT_TZ = ZoneInfo("Asia/Tashkent")
AUTO_WORK_START_HOUR = 8
AUTO_WORK_END_HOUR = 20
AUTO_DAILY_MESSAGE_LIMIT = 50
AUTO_CHAT_MIN_INTERVAL_SECONDS = 60
AUTO_SEND_PACE_SECONDS = 60


class AutoSender:
    def __init__(
        self,
        bot: Bot,
        storage: Storage,
        payment_valid_days: int,
        *,
        user_sender: Optional[UserSender] = None,
        account_manager: Optional[AccountManager] = None,
    ) -> None:
        self._bot = bot
        self._storage = storage
        self._payment_valid_days = max(0, payment_valid_days)
        self._user_sender = user_sender
        self._account_manager = account_manager
        self._personal_chats: Dict[int, str] = {}
        self._lock = asyncio.Lock()
        self._tasks: Dict[int, asyncio.Task[None]] = {}
        self._stop_events: Dict[int, asyncio.Event] = {}
        self._logger = logging.getLogger(__name__)

    async def start_if_enabled(self) -> None:
        await self.refresh_all()

    async def refresh_all(self) -> None:
        for user_id in await self._storage.list_auto_user_ids():
            await self.refresh_user(user_id)

    async def refresh_user(self, user_id: int) -> None:
        auto = await self._storage.get_auto(user_id)
        if not auto.get("is_enabled"):
            await self.stop_user(user_id)
            return
        if not await self._payments_ready(user_id):
            await self._storage.set_auto_enabled(user_id, False)
            await self.stop_user(user_id)
            return
        await self._storage.ensure_constraints(
            user_id=user_id,
            require_targets=self._user_sender is None and not auto.get("sender_account_id"),
        )
        auto = await self._storage.get_auto(user_id)
        if not auto.get("is_enabled"):
            await self.stop_user(user_id)
            return
        await self.stop_user(user_id)
        await self._start_task_for_user(user_id)

    async def stop_user(self, user_id: int) -> None:
        task: Optional[asyncio.Task[None]]
        async with self._lock:
            stop_event = self._stop_events.get(user_id)
            if stop_event:
                stop_event.set()
            task = self._tasks.get(user_id)
        if task:
            await task

    async def stop_all(self) -> None:
        for user_id in list(self._tasks.keys()):
            await self.stop_user(user_id)

    async def replace_user_sender(self, user_sender: Optional[UserSender]) -> None:
        self._user_sender = user_sender
        if user_sender is None:
            self._personal_chats = {}
        await self._storage.ensure_constraints(
            user_id=None,
            require_targets=self._user_sender is None,
        )
        await self.refresh_all()

    async def _deliver_message(
        self,
        user_id: int,
        chat_id: int,
        message: str,
        account_id: Optional[int],
    ) -> None:
        if account_id is not None:
            if not self._account_manager:
                raise RuntimeError("Личный аккаунт для рассылки недоступен.")
            account = await self._storage.get_user_account(account_id, owner_id=user_id)
            if not account:
                raise RuntimeError("Аккаунт удалён или недоступен.")
            session = account.get("session")
            if not session:
                raise RuntimeError("У аккаунта отсутствует активная сессия.")
            try:
                sender = await self._account_manager.get_sender(account)
                await sender.send_message(chat_id, message)
            except InvalidUserSessionError:
                await self._account_manager.drop_sender(account_id)
                raise
            return
        if self._user_sender:
            try:
                await self._user_sender.send_message(chat_id, message)
            except InvalidUserSessionError:
                await self._disable_shared_user_sender()
                raise
            return
        await self._bot.send_message(chat_id, message)

    async def _start_task_for_user(self, user_id: int) -> None:
        async with self._lock:
            current = self._tasks.get(user_id)
            if current and not current.done():
                return
            stop_event = asyncio.Event()
            task = asyncio.create_task(self._run_user(user_id, stop_event), name=f"auto-sender-{user_id}")
            self._tasks[user_id] = task
            self._stop_events[user_id] = stop_event

    async def _run_user(self, user_id: int, stop_event: asyncio.Event) -> None:
        try:
            while True:
                auto = await self._storage.get_auto(user_id)
                if not auto.get("is_enabled"):
                    break
                if not await self._payments_ready(user_id):
                    await self._storage.set_auto_enabled(user_id, False)
                    break
                message = auto.get("message")
                interval = int(auto.get("interval_minutes") or 0)
                targets = await self._resolve_targets(auto)
                if not message or not targets or interval <= 0:
                    await self._storage.set_auto_enabled(user_id, False)
                    break

                wait_until_work_window = self._seconds_until_work_window()
                if wait_until_work_window is not None:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=wait_until_work_window)
                        stop_event.clear()
                        break
                    except asyncio.TimeoutError:
                        continue

                account_id = auto.get("sender_account_id")
                success = 0
                errors: List[str] = []
                should_stop = False
                for index, chat_id in enumerate(targets):
                    try:
                        if index > 0:
                            try:
                                await asyncio.wait_for(stop_event.wait(), timeout=AUTO_SEND_PACE_SECONDS)
                                stop_event.clear()
                                break
                            except asyncio.TimeoutError:
                                pass
                        wait_until_work_window = self._seconds_until_work_window()
                        if wait_until_work_window is not None:
                            try:
                                await asyncio.wait_for(stop_event.wait(), timeout=wait_until_work_window)
                                stop_event.clear()
                                break
                            except asyncio.TimeoutError:
                                continue
                        now = self._now_tashkent()
                        reserved, reason = await self._storage.reserve_auto_delivery(
                            user_id=user_id,
                            chat_id=chat_id,
                            day_key=now.date().isoformat(),
                            now_iso=now.isoformat(),
                            daily_limit=AUTO_DAILY_MESSAGE_LIMIT,
                            chat_interval_seconds=AUTO_CHAT_MIN_INTERVAL_SECONDS,
                        )
                        if not reserved:
                            if reason == "daily_limit":
                                errors.append(
                                    f"Дневной лимит {AUTO_DAILY_MESSAGE_LIMIT} сообщений исчерпан."
                                )
                                break
                            if reason == "chat_rate_limit":
                                errors.append(f"Чат {chat_id}: лимит 1 сообщение в минуту.")
                                continue
                        await self._deliver_message(user_id, chat_id, message, account_id)
                        success += 1
                    except (BotKicked, ChatNotFound, Unauthorized) as exc:
                        errors.append(f"Недоступен чат {chat_id}: {exc}")
                    except InvalidUserSessionError as exc:
                        errors.append(f"Личный аккаунт Telegram недоступен: {exc}")
                        await self._storage.set_auto_enabled(user_id, False)
                        should_stop = True
                        break
                    except Exception as exc:  # pragma: no cover - network errors
                        errors.append(f"Ошибка доставки в чат {chat_id}: {exc}")
                await self._storage.update_stats(user_id, sent=success, errors=errors)
                if should_stop:
                    break

                wait_for = max(1, interval * 60)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=wait_for)
                    stop_event.clear()
                    break
                except asyncio.TimeoutError:
                    continue
        finally:
            async with self._lock:
                self._tasks.pop(user_id, None)
                if self._stop_events.get(user_id) is stop_event:
                    self._stop_events.pop(user_id, None)

    async def _payments_ready(self, user_id: int) -> bool:
        user_payment = await self._storage.has_recent_payment_for_user(
            user_id,
            within_days=self._payment_valid_days,
        )
        global_payment = await self._storage.has_recent_payment(within_days=self._payment_valid_days)
        return user_payment and global_payment

    def _now_tashkent(self) -> datetime:
        return datetime.now(TASHKENT_TZ)

    def _seconds_until_work_window(self) -> Optional[float]:
        now = self._now_tashkent()
        start = now.replace(hour=AUTO_WORK_START_HOUR, minute=0, second=0, microsecond=0)
        end = now.replace(hour=AUTO_WORK_END_HOUR, minute=0, second=0, microsecond=0)
        if start <= now < end:
            return None
        if now < start:
            next_start = start
        else:
            next_start = start + timedelta(days=1)
        return max(1.0, (next_start - now).total_seconds())

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
        except InvalidUserSessionError as exc:
            self._logger.error("Общая пользовательская сессия Telegram недоступна: %s", exc)
            await self._disable_shared_user_sender()
            return
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

    async def _resolve_targets(self, auto: dict) -> List[int]:
        account_id = auto.get("sender_account_id")
        if account_id is not None:
            known = await self._storage.list_known_chats(account_id=account_id, owner_id=auto["user_id"])
            available_ids = {int(chat_id) for chat_id in known.keys()}
            selected = [int(chat_id) for chat_id in auto.get("target_chat_ids") or []]
            if selected:
                return [chat_id for chat_id in selected if chat_id in available_ids]
            return list(available_ids)
        if self._user_sender:
            personal_chats = await self.get_personal_chats(refresh=not self._personal_chats)
            available_ids = set(personal_chats.keys())
            selected = [int(chat_id) for chat_id in auto.get("target_chat_ids") or []]
            if selected:
                return [chat_id for chat_id in selected if chat_id in available_ids]
            return list(personal_chats.keys())
        return list(auto.get("target_chat_ids") or [])

    async def _disable_shared_user_sender(self) -> None:
        sender = self._user_sender
        self._user_sender = None
        self._personal_chats = {}
        self._bot["user_sender"] = None
        audience_parser = self._bot.get("audience_parser")
        if audience_parser:
            audience_parser.set_user_sender(None)
        if sender:
            try:
                await sender.stop()
            except Exception:
                self._logger.exception("Не удалось остановить невалидную пользовательскую сессию.")
        await self._storage.ensure_constraints(user_id=None, require_targets=True)
