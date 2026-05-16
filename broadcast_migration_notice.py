import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Callable, Optional, Set

from aiogram import Bot, types
from aiogram.utils import exceptions
from dotenv import load_dotenv

from app.runtime_config import BASE_DIR, create_storage_from_env


DEFAULT_MESSAGE = (
    "Бот переехал: @atRasylon_bot\n\n"
    "Оплаты сохранены. Если у вас появляется сообщение, что нужно оплатить, "
    "заполните все детали — админ проверит и даст аппрув."
)
ENABLE_ENV_VAR = "ENABLE_MIGRATION_BROADCAST"
BROADCAST_SETTING_KEY = "migration_broadcast_notice_status"
BROADCAST_STATUS_STARTED = "started"
BROADCAST_STATUS_DONE = "done"
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send migration notice to known bot users.")
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually send messages. Without this flag the script only prints recipients.",
    )
    parser.add_argument(
        "--message",
        default=DEFAULT_MESSAGE,
        help="Message text to send.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay between messages in seconds.",
    )
    return parser.parse_args()


async def collect_known_user_ids(storage: Optional[Any] = None) -> list[int]:
    storage = storage or create_storage_from_env()
    data = await storage.get_data()
    user_ids: Set[int] = set()

    for user_id in (data.get("auto") or {}).keys():
        try:
            user_ids.add(int(user_id))
        except (TypeError, ValueError):
            pass

    for user_id in (data.get("sessions") or {}).keys():
        try:
            user_ids.add(int(user_id))
        except (TypeError, ValueError):
            pass

    for payment in (data.get("payments") or {}).values():
        try:
            user_ids.add(int(payment.get("user_id")))
        except (TypeError, ValueError):
            pass

    return sorted(user_ids)


async def send_notice_with_bot(
    bot: Bot,
    user_ids: list[int],
    message: str,
    delay: float,
    *,
    report: Callable[[str], None] = print,
) -> tuple[int, int]:
    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await bot.send_message(user_id, message)
            sent += 1
            report(f"sent {user_id}")
        except exceptions.RetryAfter as exc:
            await asyncio.sleep(exc.timeout)
            await bot.send_message(user_id, message)
            sent += 1
            report(f"sent {user_id} after retry")
        except (exceptions.BotBlocked, exceptions.UserDeactivated, exceptions.ChatNotFound) as exc:
            failed += 1
            report(f"failed {user_id}: {exc.__class__.__name__}")
        except Exception as exc:
            failed += 1
            report(f"failed {user_id}: {exc}")
        if delay > 0:
            await asyncio.sleep(delay)
    report(f"done: sent={sent}, failed={failed}, total={len(user_ids)}")
    return sent, failed


async def send_notice(user_ids: list[int], message: str, delay: float) -> tuple[int, int]:
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set.")

    bot = Bot(token=bot_token, parse_mode=types.ParseMode.HTML)
    try:
        return await send_notice_with_bot(bot, user_ids, message, delay)
    finally:
        await bot.session.close()


async def run_startup_broadcast_once(
    bot: Bot,
    storage: Any,
    *,
    message: str = DEFAULT_MESSAGE,
    delay: float = 0.1,
) -> None:
    if os.getenv(ENABLE_ENV_VAR, "").lower() not in {"1", "true", "yes"}:
        return

    claimed = await storage.claim_system_setting_if_missing(
        BROADCAST_SETTING_KEY,
        BROADCAST_STATUS_STARTED,
    )
    if not claimed:
        status = await storage.get_system_setting(BROADCAST_SETTING_KEY)
        logger.info("Migration broadcast skipped: status=%s.", status)
        return

    try:
        user_ids = await collect_known_user_ids(storage)
        logger.info("Migration broadcast started for %s recipients.", len(user_ids))
        sent, failed = await send_notice_with_bot(
            bot,
            user_ids,
            message,
            max(0.0, delay),
            report=logger.info,
        )
        await storage.set_system_setting(
            BROADCAST_SETTING_KEY,
            f"{BROADCAST_STATUS_DONE}:sent={sent}:failed={failed}",
        )
    except Exception:
        logger.exception("Migration broadcast failed after it was marked as started.")


async def main() -> None:
    args = parse_args()
    load_dotenv(Path(BASE_DIR) / ".env")
    storage = create_storage_from_env()
    user_ids = await collect_known_user_ids(storage)
    print(f"recipients: {len(user_ids)}")
    for user_id in user_ids:
        print(user_id)

    if not args.send:
        print("\ndry-run only. Run with --send to send the message.")
        return

    if os.getenv(ENABLE_ENV_VAR, "").lower() not in {"1", "true", "yes"}:
        print(
            f"\nsending is disabled. Set {ENABLE_ENV_VAR}=true and run with --send "
            "to send the message."
        )
        return

    claimed = await storage.claim_system_setting_if_missing(
        BROADCAST_SETTING_KEY,
        BROADCAST_STATUS_STARTED,
    )
    if not claimed:
        status = await storage.get_system_setting(BROADCAST_SETTING_KEY)
        print(f"\nalready used: {BROADCAST_SETTING_KEY}={status}")
        return

    await send_notice(user_ids, args.message, max(0.0, args.delay))
    await storage.set_system_setting(BROADCAST_SETTING_KEY, BROADCAST_STATUS_DONE)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
