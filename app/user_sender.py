import asyncio
import logging
from typing import Union

from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.sessions import StringSession


ChatId = Union[int, str]


class UserSender:
    """Wrapper around a Telethon client that sends messages from a user account."""

    def __init__(self, api_id: int, api_hash: str, session_string: str) -> None:
        self._client = TelegramClient(StringSession(session_string), api_id, api_hash)
        self._start_lock = asyncio.Lock()
        self._started = False
        self._logger = logging.getLogger(__name__)

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

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            await self._client.disconnect()
            self._started = False

    @property
    def client(self) -> TelegramClient:
        return self._client
