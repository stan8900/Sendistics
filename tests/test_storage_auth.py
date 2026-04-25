import tempfile
import unittest
from pathlib import Path

from app.storage import Storage


class StorageAuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.storage = Storage(Path(self._tmpdir.name) / "storage.json")

    async def test_create_account_hashes_password_and_logs_in_telegram_user(self) -> None:
        account = await self.storage.create_account(
            "Sultan_1",
            "password123",
            telegram_user_id=42,
        )

        self.assertEqual(account["username"], "sultan_1")
        self.assertNotIn("password_hash", account)
        self.assertNotIn("salt", account)
        self.assertEqual((await self.storage.get_active_account(42))["id"], account["id"])

        raw_data = await self.storage.get_data()
        stored = raw_data["accounts"][account["id"]]
        self.assertNotEqual(stored["password_hash"], "password123")
        self.assertNotEqual(stored["salt"], "")

    async def test_authenticate_account_links_new_telegram_session(self) -> None:
        created = await self.storage.create_account("team", "password123")

        self.assertIsNone(await self.storage.authenticate_account("team", "wrong"))
        logged_in = await self.storage.authenticate_account(
            "team",
            "password123",
            telegram_user_id=99,
        )

        self.assertEqual(logged_in["id"], created["id"])
        self.assertEqual((await self.storage.get_active_account(99))["id"], created["id"])

    async def test_account_auto_settings_are_isolated(self) -> None:
        first = await self.storage.create_account("first", "password123")
        second = await self.storage.create_account("second", "password123")

        await self.storage.set_account_auto_message(first["id"], "hello")

        self.assertEqual((await self.storage.get_account_auto(first["id"]))["message"], "hello")
        self.assertIsNone((await self.storage.get_account_auto(second["id"]))["message"])

    async def test_logout_clears_active_account(self) -> None:
        await self.storage.create_account("team", "password123", telegram_user_id=42)

        await self.storage.logout_telegram_user(42)

        self.assertIsNone(await self.storage.get_active_account(42))

    async def test_phone_login_creates_account_and_hides_sms_code(self) -> None:
        login = await self.storage.start_phone_login("+44 7700 900123")

        account = await self.storage.verify_phone_login(
            login["phone_number"],
            login["code"],
            telegram_user_id=42,
        )

        self.assertEqual(account["phone_number"], "+447700900123")
        self.assertEqual((await self.storage.get_active_account(42))["id"], account["id"])
        self.assertEqual(account["phone_numbers"][0]["phone_number"], "+447700900123")
        raw_data = await self.storage.get_data()
        self.assertNotIn("+447700900123", raw_data["pending_phone_logins"])

    async def test_verified_phone_login_without_sms_creates_account(self) -> None:
        account = await self.storage.login_with_verified_phone(
            "+44 7700 900123",
            telegram_user_id=42,
        )

        self.assertEqual(account["phone_number"], "+447700900123")
        self.assertEqual((await self.storage.get_active_account(42))["id"], account["id"])
        self.assertEqual(account["phone_numbers"][0]["label"], "Основной номер")

    async def test_phone_login_rejects_wrong_code(self) -> None:
        login = await self.storage.start_phone_login("+447700900123")

        account = await self.storage.verify_phone_login(
            login["phone_number"],
            "000000",
            telegram_user_id=42,
        )

        self.assertIsNone(account)
        self.assertIsNone(await self.storage.get_active_account(42))

    async def test_account_dashboard_includes_payments_and_numbers(self) -> None:
        login = await self.storage.start_phone_login("+447700900123")
        account = await self.storage.verify_phone_login(
            login["phone_number"],
            login["code"],
            telegram_user_id=42,
        )

        await self.storage.add_account_payment(
            account["id"],
            amount=10,
            currency="usd",
            status="paid",
            description="Test payment",
        )
        await self.storage.add_account_phone_number(
            account["id"],
            "+447700900124",
            label="Second",
            auto_enabled=True,
        )
        dashboard = await self.storage.get_account_dashboard(42)

        self.assertEqual(len(dashboard["payments"]), 1)
        self.assertEqual(dashboard["payments"][0]["currency"], "USD")
        self.assertEqual(len(dashboard["phone_numbers"]), 2)
        self.assertTrue(dashboard["phone_numbers"][1]["auto_enabled"])


if __name__ == "__main__":
    unittest.main()
