from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hermes.planner import (
    C2C_PLANNER_AGENT,
    MAX_CONTROL_MESSAGE_BYTES,
    build_c2c_init_message,
    build_execution_prompt,
)
from hermes.storage import SQLiteStore


class PlannerProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = str(Path(self.temporary_directory.name) / "hermes.db")
        self.store = SQLiteStore(database)
        self.store.initialize()
        task = self.store.create_task(
            {
                "prompt": "add a health endpoint",
                "planner_agent": C2C_PLANNER_AGENT,
                "planning_mode": "plan",
                "execution_agent": "cc",
            },
            default_timeout_seconds=60,
            max_timeout_seconds=600,
            default_max_attempts=1,
            default_planner_max_attempts=1,
            now=100,
        )
        self.task = self.store.claim_planning_task(30, now=101)
        assert self.task is not None
        self.assertEqual(task["id"], self.task["id"])

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_init_is_c2c_control_message_and_bounded(self) -> None:
        message = build_c2c_init_message(self.task)

        self.assertLessEqual(len(message.encode("utf-8")), MAX_CONTROL_MESSAGE_BYTES)
        self.assertIn("[C2C]", message)
        self.assertIn("STATE: INIT", message)
        self.assertIn(self.task["planner_task_id"], message)
        self.assertIn("codex-with-chatgpt Skill", message)
        self.assertIn("add a health endpoint", message)

    def test_long_goal_is_not_pasted_into_control_message(self) -> None:
        task = {**self.task, "prompt": "x" * 10_000}

        message = build_c2c_init_message(task)

        self.assertLessEqual(len(message.encode("utf-8")), MAX_CONTROL_MESSAGE_BYTES)
        self.assertTrue(
            message.endswith(
                "Skill to inspect the connected workspace and return a C2C PLAN for a Claude Code worker."
            )
        )

    def test_execution_prompt_contains_original_request_and_plan(self) -> None:
        execution_prompt = build_execution_prompt(
            "add a health endpoint", "Edit src/app.py and add a regression test."
        )

        self.assertIn("add a health endpoint", execution_prompt)
        self.assertIn("Edit src/app.py", execution_prompt)
        self.assertIn("verify the result", execution_prompt)


if __name__ == "__main__":
    unittest.main()
