import asyncio
import logging
from typing import Optional, Set

from telethon import TelegramClient, utils as telethon_utils
from telethon.sessions import StringSession
from telethon.tl import types as tl_types

from .storage import Storage


class UserDelivery:
    """MTProto-based delivery helper that works with a user session."""

    def __init__(
        self,
        *,
        api_id: int,
        api_hash: str,
        session_string: str,
        dialogs_limit: Optional[int] = None,
    ) -> None:
        self._client = TelegramClient(StringSession(session_string), api_id, api_hash)
        if dialogs_limit is not None:
            self._dialogs_limit: Optional[int] = max(1, dialogs_limit)
        else:
            self._dialogs_limit = None
        self._logger = logging.getLogger(__name__)
        self._lock = asyncio.Lock()
        self._sync_lock = asyncio.Lock()
        self._connected = False

    async def start(self) -> None:
        """Connects the underlying Telegram client."""
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError(
                "TD user session is not authorised. Regenerate TG_USER_SESSION with Telethon."
            )
        self._connected = True

    async def stop(self) -> None:
        if not self._connected:
            return
        await self._client.disconnect()
        self._connected = False

    async def send_text(self, chat_id: int, text: str) -> None:
        if not self._connected:
            raise RuntimeError("TD user client is not running.")
        async with self._lock:
            await self._client.send_message(chat_id, text)

    async def sync_known_chats(self, storage: Storage) -> Set[int]:
        """Fetches dialogs for the user account and updates available chats."""
        if not self._connected:
            raise RuntimeError("TD user client is not running.")
        async with self._sync_lock:
            dialogs_iter = self._client.iter_dialogs(limit=self._dialogs_limit)
            available_ids: Set[int] = set()
            async for dialog in dialogs_iter:
                entity = dialog.entity
                chat_id = self._extract_group_id(entity)
                if chat_id is None:
                    continue
                title = getattr(entity, "title", None) or getattr(entity, "username", None) or f"Чат {chat_id}"
                await storage.upsert_known_chat(chat_id, title)
                available_ids.add(chat_id)
            await storage.replace_delivery_ready_chat_ids(available_ids)
            return available_ids

    def _extract_group_id(self, entity: Optional[tl_types.TypePeer]) -> Optional[int]:
        if entity is None:
            return None
        if isinstance(entity, tl_types.Channel):
            if not getattr(entity, "megagroup", False) and not getattr(entity, "gigagroup", False):
                return None
        elif isinstance(entity, tl_types.Chat):
            pass
        else:
            return None
        try:
            return telethon_utils.get_peer_id(entity, add_mark=True)
        except ValueError:
            return None
