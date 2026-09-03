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

    async def list_tasks(self, limit=20):
        return [
            {
                "id": "task-1",
                "status": "running",
                "current_phase": "process_started",
                "resolved_worker_id": "worker-a",
            }
        ][:limit]

    async def create_task(
        self,
        prompt,
        user_id,
        chat_id,
        idempotency_key=None,
        planner_agent=None,
        execution_agent=None,
        target_worker_id=None,
        workdir=None,
        target_gateway_id=None,
        target_profile=None,
        planning_mode="auto",
    ):
        self.created.append(
            (
                prompt,
                user_id,
                chat_id,
                planner_agent,
                execution_agent,
                target_worker_id,
                target_gateway_id,
                target_profile,
                planning_mode,
                workdir,
            )
        )
        return {
            "id": "task-1",
            "status": "planning_pending" if planning_mode == "plan" else "pending",
            "planning_mode": planning_mode,
            "current_phase": "planning" if planning_mode == "plan" else "queued",
        }

    async def get_task(self, task_id):
        return {
            "id": task_id,
            "status": "succeeded",
            "result": "done",
            "planning_mode": "direct",
            "current_phase": "completed",
            "attempt_count": 1,
            "max_attempts": 2,
            "recent_events": [
                {
                    "event_type": "task_finished",
                    "phase": "completed",
                    "message": "task completed",
                }
            ],
        }

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
        self.assertIsNone(self.director.created[-1][3])
        self.assertIsNone(self.director.created[-1][4])
        tasks = await self.bot.handle_text("/tasks", 42, 100)
        self.assertIn("process_started", tasks)

    async def test_new_routes_planner_and_execution_agent(self) -> None:
        created = await self.bot.handle_text(
            "/new --planner codex-with-chatgpt --worker worker-a --gateway homelab "
            "--profile architect --executor codex implement feature",
            42,
            100,
        )
        self.assertIn("规划 Agent：codex-with-chatgpt", created)
        self.assertIn("执行 Agent：codex", created)
        self.assertIn("Worker：worker-a", created)
        self.assertIn("Gateway/Profile：homelab/architect", created)
        self.assertEqual("implement feature", self.director.created[-1][0])
        self.assertEqual("codex-with-chatgpt", self.director.created[-1][3])
        self.assertEqual("codex", self.director.created[-1][4])
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertEqual("homelab", self.director.created[-1][6])
        self.assertEqual("architect", self.director.created[-1][7])

    async def test_new_routes_worker_workdir(self) -> None:
        created = await self.bot.handle_text(
            "/new --worker worker-a --workdir project-a --executor cc "
            "implement feature",
            42,
            100,
        )
        self.assertIn("目录：project-a", created)
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertEqual("project-a", self.director.created[-1][9])
        self.assertEqual("claude-code", self.director.created[-1][4])

    async def test_new_keeps_agent_as_executor_alias(self) -> None:
        created = await self.bot.handle_text(
            "/new --agent claude implement feature", 42, 100
        )
        self.assertIn("执行 Agent：claude-code", created)
        self.assertEqual("claude-code", self.director.created[-1][4])

    async def test_gateway_and_profile_must_be_paired(self) -> None:
        before = len(self.director.created)
        response = await self.bot.handle_text(
            "/new --gateway homelab implement feature", 42, 100
        )
        self.assertIn("Gateway 与 Profile 必须同时指定", response)
        self.assertEqual(before, len(self.director.created))

    async def test_invalid_new_options_do_not_create_task(self) -> None:
        before = len(self.director.created)
        response = await self.bot.handle_text("/new --planner invalid task", 42, 100)
        self.assertIn("参数错误", response)
        self.assertEqual(before, len(self.director.created))

    async def test_new_accepts_tab_separator(self) -> None:
        response = await self.bot.handle_text("/new\timplement tab parsing", 42, 100)
        self.assertIn("task-1", response)
        self.assertEqual("implement tab parsing", self.director.created[-1][0])

    async def test_worker_mention_creates_direct_targeted_task(self) -> None:
        response = await self.bot.handle_text("@worker-a fix login failure", 42, 100)
        self.assertIn("直接执行", response)
        self.assertEqual("fix login failure", self.director.created[-1][0])
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertIsNone(self.director.created[-1][4])
        self.assertEqual("direct", self.director.created[-1][8])

    async def test_worker_shorthand_dispatches_without_worker_id(self) -> None:
        response = await self.bot.handle_text("@worker fix login failure", 42, 100)
        self.assertIn("直接执行", response)
        self.assertEqual("fix login failure", self.director.created[-1][0])
        self.assertIsNone(self.director.created[-1][5])
        self.assertEqual("direct", self.director.created[-1][8])

    async def test_worker_mention_preserves_execution_agent(self) -> None:
        response = await self.bot.handle_text(
            "@worker-a --executor claude fix login failure", 42, 100
        )
        self.assertIn("执行 Agent：claude-code", response)
        self.assertEqual("fix login failure", self.director.created[-1][0])
        self.assertEqual("claude-code", self.director.created[-1][4])
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertEqual("direct", self.director.created[-1][8])

    async def test_worker_mention_accepts_agent_compatibility_alias(self) -> None:
        await self.bot.handle_text(
            "@worker-a --agent claude fix login failure", 42, 100
        )
        self.assertEqual("claude-code", self.director.created[-1][4])

    async def test_worker_mention_accepts_short_codex_override(self) -> None:
        await self.bot.handle_text("@worker-a -codex fix login failure", 42, 100)
        self.assertEqual("codex", self.director.created[-1][4])

    async def test_worker_mention_accepts_short_claude_code_override(self) -> None:
        await self.bot.handle_text("@worker-a -cc fix login failure", 42, 100)
        self.assertEqual("claude-code", self.director.created[-1][4])

    async def test_worker_mention_rejects_duplicate_agent_overrides(self) -> None:
        before = len(self.director.created)
        response = await self.bot.handle_text(
            "@worker-a -cc --executor codex fix login failure", 42, 100
        )
        self.assertIn("只能指定一次", response)
        self.assertEqual(before, len(self.director.created))

    async def test_coordinator_and_worker_mentions_create_planned_task(self) -> None:
        response = await self.bot.handle_text(
            "@worker-a @Coordinator analyze then implement", 42, 100
        )
        self.assertIn("先规划后执行", response)
        self.assertEqual("analyze then implement", self.director.created[-1][0])
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertEqual("plan", self.director.created[-1][8])

    async def test_planned_worker_mention_preserves_execution_agent(self) -> None:
        await self.bot.handle_text(
            "@worker-a @Coordinator --executor claude analyze then implement",
            42,
            100,
        )
        self.assertEqual("analyze then implement", self.director.created[-1][0])
        self.assertEqual("claude-code", self.director.created[-1][4])
        self.assertEqual("worker-a", self.director.created[-1][5])
        self.assertEqual("plan", self.director.created[-1][8])

    async def test_unknown_and_multiple_workers_are_rejected(self) -> None:
        before = len(self.director.created)
        unknown = await self.bot.handle_text("@missing do work", 42, 100)
        self.assertIn("未知 Worker", unknown)
        multiple = await self.bot.handle_text("@worker-a @worker-b do work", 42, 100)
        self.assertIn("一次只能指定一个 Worker", multiple)
        self.assertEqual(before, len(self.director.created))

    async def test_duplicate_worker_mention_is_rejected(self) -> None:
        before = len(self.director.created)
        response = await self.bot.handle_text("@worker-a @worker-a do work", 42, 100)
        self.assertIn("只能指定一次", response)
        self.assertEqual(before, len(self.director.created))

    async def test_mentions_after_prompt_are_not_routing_directives(self) -> None:
        await self.bot.handle_text("tell @worker-a about this", 42, 100)
        self.assertEqual("tell @worker-a about this", self.director.created[-1][0])
        self.assertEqual("auto", self.director.created[-1][8])

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
