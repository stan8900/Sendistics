import asyncio
import logging
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from telethon import functions
from telethon.errors import FloodWaitError, RPCError

from .account_manager import AccountManager
from .storage import Storage


class InviteEngine:
    """Rate-limited invite executor working over multiple accounts."""

    def __init__(self, storage: Storage, account_manager: AccountManager) -> None:
        self._storage = storage
        self._account_manager = account_manager
        self._logger = logging.getLogger(__name__)
        self._tasks: Dict[str, asyncio.Task[None]] = {}
        self._task_lock = asyncio.Lock()

    async def start_job(
        self,
        owner_user_id: int,
        *,
        target_chat: str,
        usernames_file: Path,
        settings: Dict[str, Any],
    ) -> Dict[str, Any]:
        usernames = self._load_usernames(usernames_file)
        if not usernames:
            raise RuntimeError("Список пользователей пуст — загрузите .txt со строками @username.")
        job = await self._storage.create_invite_job(
            owner_user_id,
            target_chat=target_chat,
            usernames_file=str(usernames_file),
            settings=settings,
            total_users=len(usernames),
        )
        async with self._task_lock:
            task = asyncio.create_task(self._run_job(job["id"]), name=f"invite-job-{job['id']}")
            self._tasks[job["id"]] = task
        self._logger.info("Запущена задача инвайта %s для %s", job["id"], owner_user_id)
        return job

    async def _run_job(self, job_id: str) -> None:
        try:
            job = await self._storage.get_invite_job(job_id)
            if not job:
                return
            queue = asyncio.Queue()
            usernames = self._load_usernames(Path(job["usernames_file"]))
            for username in usernames:
                queue.put_nowait(username)
            settings = job["settings"] or {}
            invites_per_account = max(1, int(settings.get("invites_per_account", 5)))
            delay_seconds = max(0.0, float(settings.get("delay_seconds", 5.0)))
            jitter = max(0.0, float(settings.get("delay_jitter", 2.0)))
            thread_limit = max(1, int(settings.get("thread_limit", 1)))
            await self._storage.update_invite_job(
                job_id,
                status="running",
                invited_count=0,
                failed_count=0,
                last_error=None,
                started_at=datetime.utcnow().isoformat(),
            )
            accounts = await self._storage.list_user_accounts(job["owner_user_id"])
            if not accounts:
                await self._storage.update_invite_job(
                    job_id,
                    status="failed",
                    last_error="Нет подключённых аккаунтов для инвайта.",
                    finished_at=datetime.utcnow().isoformat(),
                )
                return
            workers = []
            stats = {"invited": 0, "failed": 0}
            used_accounts = accounts[: min(thread_limit, len(accounts))]
            for account in used_accounts:
                workers.append(
                    asyncio.create_task(
                        self._worker(
                            job_id,
                            account,
                            queue,
                            job["target_chat"],
                            invites_per_account,
                            delay_seconds,
                            jitter,
                            stats,
                        ),
                        name=f"inviter-{job_id}-{account['id']}",
                    )
                )
            await asyncio.gather(*workers, return_exceptions=True)
            remaining = queue.qsize()
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            finished_at = datetime.utcnow().isoformat()
            if stats["invited"] > 0:
                status = "completed"
                last_error = (
                    f"Осталось {remaining} пользователей: достигнут лимит аккаунтов за задачу."
                    if remaining
                    else None
                )
            else:
                status = "failed"
                last_error = "Не удалось пригласить пользователей"
            await self._storage.update_invite_job(
                job_id,
                status=status,
                invited_count=stats["invited"],
                failed_count=stats["failed"],
                finished_at=finished_at,
                last_error=last_error,
            )
        finally:
            async with self._task_lock:
                self._tasks.pop(job_id, None)

    async def _worker(
        self,
        job_id: str,
        account: Dict[str, Any],
        queue: asyncio.Queue,
        target_chat: str,
        invites_per_account: int,
        delay_seconds: float,
        jitter: float,
        stats: Dict[str, int],
    ) -> None:
        sender = await self._account_manager.get_sender(account)
        await sender.start()
        client = sender.client
        entity = await client.get_entity(target_chat)
        invited = 0
        while invited < invites_per_account:
            try:
                username = await asyncio.wait_for(queue.get(), timeout=3)
            except asyncio.TimeoutError:
                break
            try:
                user_entity = await client.get_entity(username)
                await client(
                    functions.channels.InviteToChannelRequest(
                        channel=entity,
                        users=[user_entity],
                    )
                )
                invited += 1
                stats["invited"] += 1
                await self._storage.update_invite_job(
                    job_id,
                    invited_count=stats["invited"],
                    failed_count=stats["failed"],
                )
                await asyncio.sleep(delay_seconds + random.uniform(0, jitter))
            except FloodWaitError as exc:
                self._logger.warning("Flood wait на аккаунте %s (%s c.)", account["id"], exc.seconds)
                await asyncio.sleep(exc.seconds)
            except RPCError as exc:
                self._logger.warning("Ошибка инвайта %s через %s: %s", username, account["id"], exc)
                stats["failed"] += 1
            except Exception as exc:  # pragma: no cover - defensive
                self._logger.exception("Непредвиденная ошибка инвайта через %s: %s", account["id"], exc)
                stats["failed"] += 1
            finally:
                queue.task_done()
        self._logger.info(
            "Аккаунт %s завершил инвайт (отправлено %s, ошибок %s)",
            account["id"],
            invited,
            stats["failed"],
        )

    def _load_usernames(self, path: Path) -> List[str]:
        if not path.exists():
            return []
        usernames: List[str] = []
        seen = set()
        with path.open("r", encoding="utf-8") as handler:
            for line in handler:
                raw = line.strip()
                if not raw:
                    continue
                if "," in raw:
                    parts = [part.strip() for part in raw.split(",")]
                    if len(parts) >= 2 and parts[0].lower() == "user_id" and parts[1].lower() == "username":
                        continue
                    username = parts[1] if len(parts) >= 2 and parts[1] else parts[0]
                else:
                    username = raw
                username = username.lstrip("@").strip()
                if not username:
                    continue
                if username.lower() in seen:
                    continue
                seen.add(username.lower())
                usernames.append(username)
        return usernames
