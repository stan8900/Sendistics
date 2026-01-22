import asyncio
import logging
from typing import Iterable, List, Optional, Set, Tuple, Union

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import Channel, Chat, DialogFilter


ChatId = Union[int, str]


class UserSender:
    """Wrapper around a Telethon client that sends messages from a user account."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str,
        *,
        allowed_folder_titles: Optional[Iterable[str]] = None,
    ) -> None:
        self._client = TelegramClient(StringSession(session_string), api_id, api_hash)
        self._start_lock = asyncio.Lock()
        self._started = False
        self._logger = logging.getLogger(__name__)
        if allowed_folder_titles:
            self._allowed_folder_titles = {
                title.lower(): title for title in allowed_folder_titles if title
            }
        else:
            self._allowed_folder_titles = {}
        self._allowed_folder_ids: Optional[Set[int]] = None

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            await self._client.start()
            if not await self._client.is_user_authorized():
                raise RuntimeError(
                    "Пользовательская сессия Telegram не авторизована. Заново сгенерируйте TG_USER_SESSION."
                )
            self._started = True

    async def send_message(self, chat_id: ChatId, message: str) -> None:
        if not self._started:
            await self.start()
        try:
            await self._client.send_message(chat_id, message)
        except RPCError as exc:
            raise RuntimeError(f"Не удалось отправить сообщение через пользовательский аккаунт: {exc}") from exc

    async def describe_self(self) -> str:
        if not self._started:
            await self.start()
        me = await self._client.get_me()
        if not me:
            return "неизвестный пользователь"
        username = f"@{me.username}" if getattr(me, "username", None) else None
        full_name = " ".join(filter(None, [me.first_name, me.last_name])) or str(me.id)
        return f"{full_name} {username}" if username else full_name

    async def list_accessible_chats(self) -> List[Tuple[int, str]]:
        if not self._started:
            await self.start()
        chats: List[Tuple[int, str]] = []
        allowed_folders = await self._resolve_folder_ids()
        async for dialog in self._client.iter_dialogs():
            if allowed_folders and dialog.folder_id not in allowed_folders:
                continue
            entity = dialog.entity
            title = dialog.name or getattr(entity, "title", None) or getattr(entity, "username", None)
            if isinstance(entity, Chat):
                chats.append((entity.id, title or f"Чат {entity.id}"))
            elif isinstance(entity, Channel) and not getattr(entity, "broadcast", False):
                chats.append((entity.id, title or f"Чат {entity.id}"))
        return chats

    async def _resolve_folder_ids(self) -> Set[int]:
        if not self._allowed_folder_titles:
            return set()
        if self._allowed_folder_ids is not None:
            return set(self._allowed_folder_ids)
        try:
            response = await self._client(GetDialogFiltersRequest())
        except RPCError:
            self._logger.exception("Не удалось получить список папок диалогов Telegram.")
            self._allowed_folder_ids = set()
            return set()
        filters = []
        if isinstance(response, list):
            filters = response
        elif hasattr(response, "filters"):
            filters = response.filters  # type: ignore[attr-defined]
        matched: Set[int] = set()
        matched_titles: Set[str] = set()
        for item in filters:
            if not isinstance(item, DialogFilter):
                continue
            folder_id = getattr(item, "id", None)
            title = (getattr(item, "title", None) or "").strip()
            if not folder_id or not title:
                continue
            lower_title = title.lower()
            if lower_title in self._allowed_folder_titles:
                matched.add(folder_id)
                matched_titles.add(self._allowed_folder_titles[lower_title])
        if not matched:
            self._logger.warning(
                "Папки %s не найдены среди фильтров Telegram. Рассылка не будет выполняться.",
                ", ".join(self._allowed_folder_titles.values()),
            )
        else:
            self._logger.info(
                "Используем только чаты из папок: %s",
                ", ".join(sorted(matched_titles)),
            )
        self._allowed_folder_ids = matched
        return set(matched)

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            await self._client.disconnect()
            self._started = False

    @property
    def client(self) -> TelegramClient:
        return self._client
