import asyncio
import tempfile
import unittest
from pathlib import Path

from app.storage import Storage


class StorageExtensionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "storage.db"
        self.storage = Storage(db_path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_register_and_list_audience_dump(self) -> None:
        async def runner() -> None:
            dump = await self.storage.register_audience_dump(
                123,
                source="test_channel",
                file_path="/tmp/sample.txt",
                total_users=42,
            )
            self.assertEqual(dump["source"], "test_channel")
            dumps = await self.storage.list_audience_dumps(123, limit=5)
            self.assertEqual(len(dumps), 1)
            self.assertEqual(dumps[0]["total_users"], 42)
            self.assertTrue(dumps[0]["file_path"].endswith("sample.txt"))

        asyncio.run(runner())

    def test_create_update_and_list_invite_jobs(self) -> None:
        async def runner() -> None:
            job = await self.storage.create_invite_job(
                555,
                target_chat="@target",
                usernames_file="/tmp/users.txt",
                settings={"thread_limit": 2},
                total_users=10,
            )
            self.assertEqual(job["status"], "pending")
            await self.storage.update_invite_job(
                job["id"],
                status="running",
                invited_count=4,
                failed_count=1,
            )
            updated = await self.storage.get_invite_job(job["id"])
            self.assertIsNotNone(updated)
            self.assertEqual(updated["status"], "running")
            self.assertEqual(updated["invited_count"], 4)
            jobs = await self.storage.list_invite_jobs(555)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["failed_count"], 1)

        asyncio.run(runner())


if __name__ == "__main__":
    unittest.main()
