import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon.errors import RPCError
from telethon.tl.custom import Message
from telethon.tl.types import Channel, Chat, User

from .account_manager import AccountManager
from .storage import Storage
from .user_sender import UserSender


class AudienceParser:
    """Collects Telegram usernames from channel comments."""

    def __init__(
        self,
        storage: Storage,
        *,
        base_dir: Path,
        user_sender: Optional[UserSender] = None,
        account_manager: Optional[AccountManager] = None,
    ) -> None:
        self._storage = storage
        self._base_dir = Path(base_dir)
        self._user_sender = user_sender
        self._account_manager = account_manager
        self._logger = logging.getLogger(__name__)
        self._client_lock = asyncio.Lock()

    def set_user_sender(self, user_sender: Optional[UserSender]) -> None:
        self._user_sender = user_sender

    async def parse_comments(
        self,
        owner_user_id: int,
        *,
        source: str,
        limit: int = 20,
        account_id: Optional[int] = None,
    ) -> Dict[str, str]:
        """Parse usernames from channel comments."""
        client, account_label = await self._resolve_client(owner_user_id, account_id=account_id)
        limit = max(1, min(500, int(limit)))
        source = source.strip()
        collected: Dict[str, Dict[str, str]] = {}
        entity = await client.get_entity(source)
        messages = []
        async for message in client.iter_messages(entity, limit=limit):
            messages.append(message)
        messages.reverse()  # from old to new to reduce duplicates
        for message in messages:
            await self._collect_comments(client, entity, message, collected)
        output_path = await self._store_results(owner_user_id, source, collected, suffix="comments")
        dump = await self._storage.register_audience_dump(
            owner_user_id,
            source=source,
            file_path=str(output_path),
            total_users=len(collected),
        )
        self._logger.info(
            "Audience dump %s created by %s (%s records) using %s",
            dump["id"],
            owner_user_id,
            dump["total_users"],
            account_label or "shared session",
        )
        return dump

    async def parse_group_members(
        self,
        owner_user_id: int,
        *,
        group: Any,
        account_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        client, account_label = await self._resolve_client(owner_user_id, account_id=account_id)
        entity = await client.get_entity(group)
        collected: Dict[str, Dict[str, str]] = {}
        async for participant in client.iter_participants(entity):
            username = (participant.username or "").strip()
            key = username.lower() if username else str(getattr(participant, "id", ""))
            collected[key] = {
                "username": username,
                "user_id": str(getattr(participant, "id", "")),
                "access_hash": str(getattr(participant, "access_hash", "")),
            }
        title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
        output_path = await self._store_results(owner_user_id, title, collected, suffix="group")
        dump = await self._storage.register_audience_dump(
            owner_user_id,
            source=title,
            file_path=str(output_path),
            total_users=len(collected),
        )
        self._logger.info(
            "Group dump %s created by %s (%s records) using %s",
            dump["id"],
            owner_user_id,
            dump["total_users"],
            account_label or "shared session",
        )
        return dump

    async def list_personal_groups(
        self,
        owner_user_id: int,
        *,
        account_id: Optional[int] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        client, _ = await self._resolve_client(owner_user_id, account_id=account_id)
        groups: List[Dict[str, Any]] = []
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, User):
                continue
            if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                continue
            if isinstance(entity, Channel) and not getattr(entity, "megagroup", False):
                continue
            if isinstance(entity, Chat) or (isinstance(entity, Channel) and getattr(entity, "megagroup", False)):
                groups.append(
                    {
                        "id": int(entity.id),
                        "title": dialog.name or getattr(entity, "title", f"Chat {entity.id}"),
                        "username": getattr(entity, "username", None),
                    }
                )
            if len(groups) >= limit:
                break
        return groups

    async def _collect_comments(
        self,
        client,
        entity,
        message: Message,
        collected: Dict[str, Dict[str, str]],
    ) -> None:
        if not message or not getattr(message, "replies", None):
            return
        try:
            async for reply in client.iter_messages(
                entity,
                reply_to=message.id,
            ):
                sender: Optional[User] = await reply.get_sender()
                if not sender:
                    continue
                username = (sender.username or "").strip()
                if not username:
                    continue
                normalized = username.lower()
                collected[normalized] = {
                    "username": username,
                    "user_id": str(getattr(sender, "id", "")),
                    "access_hash": str(getattr(sender, "access_hash", "")),
                }
        except RPCError as exc:
            self._logger.warning("Failed to iterate comments for message %s: %s", message.id, exc)

    async def _store_results(
        self,
        owner_user_id: int,
        source: str,
        collected: Dict[str, Dict[str, str]],
        *,
        suffix: str = "comments",
    ) -> Path:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_source = self._sanitize(source)
        directory = self._base_dir / "data" / "dumps"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{safe_source}_{owner_user_id}_{suffix}_{timestamp}.txt"
        with path.open("w", encoding="utf-8") as handler:
            handler.write("user_id,username,access_hash\n")
            for info in collected.values():
                handler.write(f"{info['user_id']},{info['username']},{info['access_hash']}\n")
        return path

    async def _resolve_client(
        self,
        owner_user_id: int,
        *,
        account_id: Optional[int] = None,
    ) -> Tuple[Any, Optional[str]]:
        async with self._client_lock:
            if account_id and self._account_manager:
                account = await self._storage.get_user_account(account_id, owner_id=owner_user_id)
                if account:
                    sender = await self._account_manager.get_sender(account)
                    await sender.start()
                    return sender.client, f"account #{account['id']}"
            if self._user_sender:
                await self._user_sender.start()
                return self._user_sender.client, "shared personal session"
            if self._account_manager:
                accounts = await self._storage.list_user_accounts(owner_user_id)
                if accounts:
                    account = accounts[0]
                    sender = await self._account_manager.get_sender(account)
                    await sender.start()
                    return sender.client, f"account #{account['id']}"
        raise RuntimeError("Нет доступных аккаунтов для подключения к MTProto.")

    def _sanitize(self, value: str) -> str:
        sanitized = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip("@ "))
        return sanitized or "channel"
