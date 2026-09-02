from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from hermes.config import UnifiedWorkerSettings, WorkerSettings


class ConfigTests(unittest.TestCase):
    def test_unified_settings_preserve_legacy_command_worker_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(
                os.environ,
                {
                    "HERMES_ENV_FILE": str(Path(temporary_directory) / "missing.env"),
                    "HERMES_WORKER_ID": "legacy-worker",
                    "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                    "HERMES_WORKER_COMMAND": "legacy-agent --prompt {prompt}",
                },
                clear=True,
            ):
                settings = UnifiedWorkerSettings.from_env()

        self.assertEqual("default", settings.default_agent)
        self.assertEqual(["legacy-agent", "--prompt", "{prompt}"], settings.command)
        self.assertEqual({}, settings.agent_commands)
        self.assertFalse(settings.gateway_enabled)
        self.assertEqual("", settings.gateway_id)

    def test_explicit_hermes_default_enables_gateway_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(
                os.environ,
                {
                    "HERMES_ENV_FILE": str(Path(temporary_directory) / "missing.env"),
                    "HERMES_WORKER_ID": "unified-worker",
                    "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                    "HERMES_WORKER_DEFAULT_AGENT": "hermes",
                },
                clear=True,
            ):
                settings = UnifiedWorkerSettings.from_env()

        self.assertEqual("hermes", settings.default_agent)
        self.assertTrue(settings.gateway_enabled)
        self.assertTrue(settings.gateway_id)

    def test_legacy_agent_commands_alias_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(
                os.environ,
                {
                    "HERMES_ENV_FILE": str(Path(temporary_directory) / "missing.env"),
                    "HERMES_WORKER_ID": "alias-worker",
                    "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                    "HERMES_AGENT_COMMANDS": json.dumps(
                        {"claude": ["claude", "-p", "{prompt}"]}
                    ),
                },
                clear=True,
            ):
                settings = WorkerSettings.from_env()

        self.assertEqual(
            ["claude", "-p", "{prompt}"], settings.agent_commands["claude"]
        )

    def test_unified_settings_load_default_agents_and_gateway(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            with patch.dict(
                os.environ,
                {
                    "HERMES_ENV_FILE": str(Path(temporary_directory) / "missing.env"),
                    "HERMES_WORKER_ID": "unified-worker",
                    "HERMES_WORKER_SHARED_SECRET": "worker-secret",
                    "HERMES_WORKER_DEFAULT_AGENT": "cc",
                    "HERMES_GATEWAY_ID": "local-hermes",
                    "HERMES_GATEWAY_PROFILES": "default,architect",
                },
                clear=True,
            ):
                settings = UnifiedWorkerSettings.from_env()

        self.assertEqual("claude-code", settings.default_agent)
        self.assertEqual(
            ["codex", "exec", "{prompt}"], settings.agent_commands["codex"]
        )
        self.assertEqual(["default", "architect"], settings.profiles)


if __name__ == "__main__":
    unittest.main()
