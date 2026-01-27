import asyncio
import logging
from typing import Awaitable, Callable, Iterable, List, Optional, Set, Tuple, TypeVar, Union

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.types import Channel, Chat, DialogFilter


ChatId = Union[int, str]
T = TypeVar("T")


class UserSender:
    """Wrapper around a Telethon client that sends messages from a user account."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str,
        *,
        allowed_folder_titles: Optional[Iterable[str]] = None,
        allowed_chat_identifiers: Optional[Iterable[str]] = None,
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
        self._allowed_chat_ids: Set[int] = set()
        self._allowed_chat_usernames: Set[str] = set()
        self._allowed_chat_titles: Set[str] = set()
        if allowed_chat_identifiers:
            for raw in allowed_chat_identifiers:
                self._register_chat_identifier(raw)
        self._retry_attempts = 3
        self._retry_delay = 2.0

    async def start(self) -> None:
        await self._ensure_ready()

    async def send_message(self, chat_id: ChatId, message: str) -> None:
        async def _send() -> None:
            await self._client.send_message(chat_id, message)

        try:
            await self._run_with_reconnect("отправить сообщение", _send)
        except RPCError as exc:
            raise RuntimeError(f"Не удалось отправить сообщение через пользовательский аккаунт: {exc}") from exc

    async def describe_self(self) -> str:
        me = await self._run_with_reconnect("получить данные профиля", self._client.get_me)
        if not me:
            return "неизвестный пользователь"
        username = f"@{me.username}" if getattr(me, "username", None) else None
        full_name = " ".join(filter(None, [me.first_name, me.last_name])) or str(me.id)
        return f"{full_name} {username}" if username else full_name

    async def list_accessible_chats(self) -> List[Tuple[int, str]]:
        async def _collect() -> List[Tuple[int, str]]:
            chats: List[Tuple[int, str]] = []
            allowed_folders = await self._resolve_folder_ids()
            enforce_filter = bool(self._allowed_folder_titles)
            enforce_chat_filter = bool(
                self._allowed_chat_ids or self._allowed_chat_usernames or self._allowed_chat_titles
            )
            async for dialog in self._client.iter_dialogs():
                if enforce_filter and dialog.folder_id not in allowed_folders:
                    continue
                entity = dialog.entity
                title = dialog.name or getattr(entity, "title", None) or getattr(entity, "username", None)
                if enforce_chat_filter and not self._matches_chat_filter(entity, title):
                    continue
                if isinstance(entity, Chat):
                    chats.append((entity.id, title or f"Чат {entity.id}"))
                elif isinstance(entity, Channel) and not getattr(entity, "broadcast", False):
                    chats.append((entity.id, title or f"Чат {entity.id}"))
            return chats

        return await self._run_with_reconnect("получить список чатов", _collect)

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

    def _register_chat_identifier(self, raw_value: str) -> None:
        if not raw_value:
            return
        value = raw_value.strip()
        if not value:
            return
        if value.startswith("https://") or value.startswith("http://"):
            slug = value.rstrip("/").rsplit("/", 1)[-1]
            value = slug
        if value.lower().startswith("t.me/"):
            value = value.split("/", 1)[-1]
        if value.startswith("+"):
            # joinchat link slug, cannot resolve via username
            # keep as title fallback
            pass
        if value.startswith("@"):
            value = value[1:]
        if value.lstrip("-").isdigit():
            try:
                self._allowed_chat_ids.add(int(value))
                return
            except ValueError:
                pass
        normalized = value.casefold()
        if normalized:
            self._allowed_chat_usernames.add(normalized.strip("@"))
            self._allowed_chat_titles.add(normalized)

    def _matches_chat_filter(self, entity: Union[Chat, Channel], title: Optional[str]) -> bool:
        if not (self._allowed_chat_ids or self._allowed_chat_usernames or self._allowed_chat_titles):
            return True
        if self._allowed_chat_ids and getattr(entity, "id", None) in self._allowed_chat_ids:
            return True
        username = getattr(entity, "username", None)
        if username:
            normalized = username.casefold().lstrip("@")
            if normalized in self._allowed_chat_usernames:
                return True
        name = (title or "").strip()
        if name:
            normalized_title = name.casefold()
            if normalized_title in self._allowed_chat_titles:
                return True
        return False

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            await self._client.disconnect()
            self._started = False

    @property
    def client(self) -> TelegramClient:
        return self._client

    async def _ensure_ready(self) -> None:
        async with self._start_lock:
            if not self._started:
                await self._client.start()
                if not await self._client.is_user_authorized():
                    raise RuntimeError(
                        "Пользовательская сессия Telegram не авторизована. Заново сгенерируйте TG_USER_SESSION."
                )
                self._started = True
                return
            if not self._client.is_connected():
                await self._client.connect()
                if not await self._client.is_user_authorized():
                    raise RuntimeError(
                        "Пользовательская сессия Telegram не авторизована. Заново сгенерируйте TG_USER_SESSION."
                    )

    async def _run_with_reconnect(self, action: str, func: Callable[[], Awaitable[T]]) -> T:
        last_exc: Optional[ConnectionError] = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                await self._ensure_ready()
                return await func()
            except ConnectionError as exc:
                last_exc = exc
                if attempt >= self._retry_attempts:
                    break
                await self._handle_disconnect(exc, action, attempt)
        if last_exc:
            raise RuntimeError(f"Не удалось {action}: {last_exc}") from last_exc
        raise RuntimeError(f"Не удалось {action}: неизвестная ошибка")

    async def _handle_disconnect(self, exc: Exception, action: str, attempt: int) -> None:
        delay = min(self._retry_delay * attempt, self._retry_delay * 3)
        self._logger.warning(
            "Не удалось %s через пользовательский аккаунт (попытка %s/%s): %s. Повторим через %.1f с.",
            action,
            attempt,
            self._retry_attempts,
            exc,
            delay,
        )
        await self._reset_client_connection()
        await asyncio.sleep(delay)

    async def _reset_client_connection(self) -> None:
        async with self._start_lock:
            try:
                await self._client.disconnect()
            except Exception:
                self._logger.exception("Ошибка при отключении пользовательского аккаунта Telegram.")
            self._started = False
