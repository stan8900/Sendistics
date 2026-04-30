import asyncio
import unittest

from app.auto_sender import AutoSender
from app.user_sender import InvalidUserSessionError


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


class InvalidSessionTest(unittest.TestCase):
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
