import asyncio
from typing import Dict, Optional

from .user_sender import UserSender


class AccountManager:
    """Manages Telethon sessions for user-provided accounts."""

    def __init__(self, api_id: int, api_hash: str) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._senders: Dict[int, UserSender] = {}
        self._sessions: Dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def get_sender(self, account_id: int, session_string: str) -> UserSender:
        async with self._lock:
            sender = self._senders.get(account_id)
            current_session = self._sessions.get(account_id)
            if sender and current_session != session_string:
                await sender.stop()
                sender = None
                self._senders.pop(account_id, None)
                self._sessions.pop(account_id, None)
            if sender is None:
                sender = UserSender(self._api_id, self._api_hash, session_string)
                self._senders[account_id] = sender
                self._sessions[account_id] = session_string
        await sender.start()
        return sender

    async def drop_sender(self, account_id: int) -> None:
        async with self._lock:
            sender = self._senders.pop(account_id, None)
            self._sessions.pop(account_id, None)
        if sender:
            await sender.stop()

    async def stop_all(self) -> None:
        async with self._lock:
            senders = list(self._senders.values())
            self._senders.clear()
            self._sessions.clear()
        for sender in senders:
            await sender.stop()
