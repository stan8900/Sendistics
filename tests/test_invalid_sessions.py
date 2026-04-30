import asyncio
import os
import unittest
from datetime import datetime, timezone

from app.auto_sender import AutoSender
from app.user_sender import InvalidUserSessionError, UserSender


class FakeBot(dict):
    async def send_message(self, chat_id: int, message: str) -> None:
        raise AssertionError("bot fallback should not be called after selecting user sender")


class FakeAudienceParser:
    def __init__(self) -> None:
        self.user_sender = object()

    def set_user_sender(self, user_sender) -> None:
        self.user_sender = user_sender


class FakeStorage:
    def __init__(self) -> None:
        self.ensure_constraints_calls = []

    async def ensure_constraints(self, **kwargs) -> None:
        self.ensure_constraints_calls.append(kwargs)


class RevokedSender:
    def __init__(self) -> None:
        self.stopped = False

    async def list_accessible_chats(self):
        raise InvalidUserSessionError("revoked")

    async def send_message(self, chat_id: int, message: str) -> None:
        raise InvalidUserSessionError("revoked")

    async def stop(self) -> None:
        self.stopped = True


class UnauthorizedClient:
    def __init__(self) -> None:
        self.connected = False
        self.disconnected = False
        self.start_called = False

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connected = True

    async def start(self) -> None:
        self.start_called = True
        raise AssertionError("Telethon interactive start should not be called")

    async def is_user_authorized(self) -> bool:
        return False

    async def disconnect(self) -> None:
        self.disconnected = True
        self.connected = False


class InvalidSessionTest(unittest.TestCase):
    def test_user_sender_does_not_prompt_for_unauthorized_session(self) -> None:
        async def runner() -> None:
            sender = UserSender(12345, "hash", "")
            client = UnauthorizedClient()
            sender._client = client

            with self.assertRaises(InvalidUserSessionError):
                await sender.start()

            self.assertFalse(client.start_called)
            self.assertTrue(client.disconnected)

        asyncio.run(runner())

    def test_sleep_mode_uses_tashkent_timezone(self) -> None:
        os.environ.setdefault("BOT_TOKEN", "123:abc")
        import bot

        original_until = bot.BOT_SLEEP_UNTIL_RAW
        original_from = bot.BOT_SLEEP_FROM_RAW
        original_to = bot.BOT_SLEEP_TO_RAW
        original_timezone = bot.BOT_SLEEP_TIMEZONE_RAW
        try:
            bot.BOT_SLEEP_UNTIL_RAW = None
            bot.BOT_SLEEP_FROM_RAW = "00:00"
            bot.BOT_SLEEP_TO_RAW = "09:00"
            bot.BOT_SLEEP_TIMEZONE_RAW = "Asia/Tashkent"

            sleep_until = bot.get_active_sleep_until(datetime(2026, 4, 30, 19, 13, tzinfo=timezone.utc))

            self.assertIsNotNone(sleep_until)
            self.assertEqual(sleep_until.strftime("%Y-%m-%d %H:%M %z"), "2026-05-01 09:00 +0500")
            self.assertEqual(bot.build_sleep_message(sleep_until), "Бот находится в режиме спячки до 01.05.2026 09:00. Напишите позже.")
        finally:
            bot.BOT_SLEEP_UNTIL_RAW = original_until
            bot.BOT_SLEEP_FROM_RAW = original_from
            bot.BOT_SLEEP_TO_RAW = original_to
            bot.BOT_SLEEP_TIMEZONE_RAW = original_timezone

    def test_refresh_personal_chats_disables_revoked_shared_sender(self) -> None:
        async def runner() -> None:
            sender = RevokedSender()
            storage = FakeStorage()
            parser = FakeAudienceParser()
            bot = FakeBot(user_sender=sender, audience_parser=parser)
            auto_sender = AutoSender(bot, storage, payment_valid_days=30, user_sender=sender)

            await auto_sender.get_personal_chats(refresh=True)

            self.assertIsNone(bot["user_sender"])
            self.assertIsNone(parser.user_sender)
            self.assertTrue(sender.stopped)
            self.assertEqual(storage.ensure_constraints_calls, [{"user_id": None, "require_targets": True}])
            self.assertEqual(await auto_sender.get_personal_chats(), {})

        asyncio.run(runner())

    def test_shared_delivery_disables_revoked_sender(self) -> None:
        async def runner() -> None:
            sender = RevokedSender()
            storage = FakeStorage()
            bot = FakeBot(user_sender=sender)
            auto_sender = AutoSender(bot, storage, payment_valid_days=30, user_sender=sender)

            with self.assertRaises(InvalidUserSessionError):
                await auto_sender._deliver_message(1, -100, "hello", None)

            self.assertIsNone(bot["user_sender"])
            self.assertTrue(sender.stopped)
            self.assertEqual(storage.ensure_constraints_calls, [{"user_id": None, "require_targets": True}])

        asyncio.run(runner())


if __name__ == "__main__":
    unittest.main()
