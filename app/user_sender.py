import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Union

import socks
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat


ChatId = Union[int, str]
ProxyTuple = Tuple[int, str, int, bool, Optional[str], Optional[str]]


class UserSender:
    """Wrapper around a Telethon client that sends messages from a user account."""

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_string: str,
        *,
        proxy: Optional[Dict[str, Union[str, int]]] = None,
    ) -> None:
        self._proxy = self._build_proxy(proxy)
        self._client = TelegramClient(
            StringSession(session_string),
            api_id,
            api_hash,
            proxy=self._proxy,
            connection_retries=2,
            request_retries=2,
            timeout=10,
        )
        self._start_lock = asyncio.Lock()
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                if not self._client.is_connected():
                    await self._client.connect()
                return
            await self._client.start()
            if not await self._client.is_user_authorized():
                raise RuntimeError(
                    "Пользовательская сессия Telegram не авторизована. Заново сгенерируйте TG_USER_SESSION."
                )
            self._started = True

    async def send_message(self, chat_id: ChatId, message: str) -> None:
        await self.start()
        try:
            await self._client.send_message(chat_id, message)
        except RPCError as exc:
            raise RuntimeError(f"Не удалось отправить сообщение через пользовательский аккаунт: {exc}") from exc

    async def describe_self(self) -> str:
        await self.start()
        me = await self._client.get_me()
        if not me:
            return "неизвестный пользователь"
        username = f"@{me.username}" if getattr(me, "username", None) else None
        full_name = " ".join(filter(None, [me.first_name, me.last_name])) or str(me.id)
        return f"{full_name} {username}" if username else full_name

    async def list_accessible_chats(self) -> List[Tuple[int, str]]:
        await self.start()
        chats: List[Tuple[int, str]] = []
        async for dialog in self._client.iter_dialogs():
            entity = dialog.entity
            title = dialog.name or getattr(entity, "title", None) or getattr(entity, "username", None)
            if isinstance(entity, Chat):
                chats.append((entity.id, title or f"Чат {entity.id}"))
            elif isinstance(entity, Channel) and not getattr(entity, "broadcast", False):
                chats.append((entity.id, title or f"Чат {entity.id}"))
        return chats

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            await self._client.disconnect()
            self._started = False

    @property
    def client(self) -> TelegramClient:
        return self._client

    def _build_proxy(self, proxy: Optional[Dict[str, Union[str, int]]]) -> Optional[ProxyTuple]:
        if not proxy:
            return None
        host = str(proxy.get("host") or "").strip()
        port_raw = proxy.get("port")
        if not host:
            return None
        try:
            port = int(port_raw)
        except (TypeError, ValueError):
            return None
        proxy_type = str(proxy.get("type") or "socks5").lower()
        type_map = {
            "socks5": socks.SOCKS5,
            "socks4": socks.SOCKS4,
            "http": socks.HTTP,
        }
        resolved_type = type_map.get(proxy_type, socks.SOCKS5)
        username = proxy.get("username") if proxy.get("username") else None
        password = proxy.get("password") if proxy.get("password") else None
        return resolved_type, host, port, True, username, password
