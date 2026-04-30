import asyncio
from typing import Any, Dict, Mapping, Optional, Tuple

from .user_sender import InvalidUserSessionError, UserSender


def get_account_proxy(account: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    host = str(account.get("proxy_host") or "").strip()
    port_raw = account.get("proxy_port")
    if not host:
        return None
    try:
        port = int(port_raw)
    except (TypeError, ValueError):
        return None
    if port <= 0:
        return None
    proxy_type = str(account.get("proxy_type") or "socks5").lower()
    username = (account.get("proxy_username") or "").strip() or None
    password = (account.get("proxy_password") or "").strip() or None
    return {
        "type": proxy_type,
        "host": host,
        "port": port,
        "username": username,
        "password": password,
    }


def _proxy_signature(proxy: Optional[Dict[str, Any]]) -> Optional[Tuple[str, str, int, str, str]]:
    if not proxy:
        return None
    return (
        str(proxy.get("type") or "").lower(),
        str(proxy.get("host") or ""),
        int(proxy.get("port") or 0),
        str(proxy.get("username") or ""),
        str(proxy.get("password") or ""),
    )


class AccountManager:
    """Manages Telethon sessions for user-provided accounts."""

    def __init__(self, api_id: int, api_hash: str) -> None:
        self._api_id = api_id
        self._api_hash = api_hash
        self._senders: Dict[int, UserSender] = {}
        self._configs: Dict[int, Tuple[str, Optional[Tuple[str, str, int, str, str]]]] = {}
        self._lock = asyncio.Lock()

    async def get_sender(self, account: Mapping[str, Any]) -> UserSender:
        account_id = int(account["id"])
        session_string = account.get("session")
        if not session_string:
            raise ValueError(f"Account {account_id} имеет пустую сессию.")
        proxy = get_account_proxy(account)
        desired_signature = (session_string, _proxy_signature(proxy))
        async with self._lock:
            sender = self._senders.get(account_id)
            current_signature = self._configs.get(account_id)
            if sender and current_signature != desired_signature:
                await sender.stop()
                sender = None
                self._senders.pop(account_id, None)
                self._configs.pop(account_id, None)
            if sender is None:
                sender = UserSender(self._api_id, self._api_hash, session_string, proxy=proxy)
                self._senders[account_id] = sender
                self._configs[account_id] = desired_signature
        try:
            await sender.start()
        except InvalidUserSessionError:
            async with self._lock:
                if self._senders.get(account_id) is sender:
                    self._senders.pop(account_id, None)
                    self._configs.pop(account_id, None)
            raise
        return sender

    async def drop_sender(self, account_id: int) -> None:
        async with self._lock:
            sender = self._senders.pop(account_id, None)
            self._configs.pop(account_id, None)
        if sender:
            await sender.stop()

    async def stop_all(self) -> None:
        async with self._lock:
            senders = list(self._senders.values())
            self._senders.clear()
            self._configs.clear()
        for sender in senders:
            await sender.stop()
