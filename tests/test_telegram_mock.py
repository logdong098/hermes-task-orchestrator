from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from hermes.config import TelegramSettings
from hermes.telegram_bot import DirectorBot, TelegramGateway


class FakeDirector:
    def __init__(self) -> None:
        self.created = []

    async def list_workers(self):
        return [
            {
                "worker_id": "worker-a",
                "status": "online",
                "max_concurrency": 1,
            }
        ]

    async def create_task(self, prompt, user_id, chat_id, idempotency_key=None):
        self.created.append((prompt, user_id, chat_id))
        return {"id": "task-1", "status": "pending"}

    async def get_task(self, task_id):
        return {"id": task_id, "status": "succeeded", "result": "done"}

    async def cancel_task(self, task_id):
        return {"id": task_id, "status": "cancel_requested"}


class FakeTelegram:
    def __init__(self) -> None:
        self.messages = []

    async def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


class TelegramMockTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.director = FakeDirector()
        self.telegram = FakeTelegram()
        settings = TelegramSettings(allowed_user_ids=[42])
        self.bot = DirectorBot(settings, self.director, self.telegram)

    async def test_commands_work_without_token_or_network(self) -> None:
        agents = await self.bot.handle_text("/agents", 42, 100)
        self.assertIn("worker-a", agents)
        created = await self.bot.handle_text("/new inspect logs", 42, 100)
        self.assertIn("task-1", created)
        status = await self.bot.handle_text("/status task-1", 42, 100)
        self.assertIn("done", status)
        cancelled = await self.bot.handle_text("/cancel task-1", 42, 100)
        self.assertIn("cancel_requested", cancelled)
        plain = await self.bot.handle_text("plain task", 42, 100)
        self.assertIn("task-1", plain)
        self.assertEqual("plain task", self.director.created[-1][0])

    async def test_allowlist_rejects_unknown_user(self) -> None:
        await self.bot.handle_update(
            {
                "message": {
                    "text": "secret task",
                    "from": {"id": 99},
                    "chat": {"id": 100},
                }
            }
        )
        self.assertEqual([], self.director.created)
        self.assertIn("无权访问", self.telegram.messages[-1][1])

    async def test_offset_is_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            settings = TelegramSettings(
                allowed_user_ids=[42],
                offset_path=str(Path(temporary_directory) / "offset"),
            )
            bot = DirectorBot(settings, self.director, self.telegram)
            self.assertIsNone(bot.load_offset())
            bot.save_offset(123)
            self.assertEqual(123, bot.load_offset())

    async def test_telegram_ok_false_is_an_error(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"ok": False, "description": "denied"})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gateway = TelegramGateway("not-a-real-token", client)
        try:
            with self.assertRaises(RuntimeError):
                await gateway.send_message(100, "hello")
        finally:
            await client.aclose()


if __name__ == "__main__":
    unittest.main()
