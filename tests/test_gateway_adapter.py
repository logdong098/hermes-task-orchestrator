import json
import unittest
from urllib.parse import urlsplit

import httpx

from hermes.gateway_adapter import GatewayAdapter


class GatewayAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_key_body_and_quoted_paths(self):
        seen = []

        def handler(request):
            seen.append(request)
            if request.method == "POST":
                return httpx.Response(200, json={"id": "run/x", "status": "queued"})
            return httpx.Response(200, json={"status": "succeeded"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://gateway"
        )
        adapter = GatewayAdapter(
            "http://gateway", "fallback", {"a/b": "profile-key"}, client=client
        )
        await adapter.start_run("a/b", "hello", "task-1-2")
        await adapter.get_run("a/b", "run/x")
        self.assertEqual("/p/a%2Fb/v1/runs", urlsplit(str(seen[0].url)).path)
        self.assertEqual({"input": "hello"}, json.loads(seen[0].content))
        self.assertEqual("Bearer profile-key", seen[0].headers["authorization"])
        self.assertEqual("task-1-2", seen[0].headers["idempotency-key"])
        self.assertEqual("/p/a%2Fb/v1/runs/run%2Fx", urlsplit(str(seen[1].url)).path)
        await client.aclose()

    async def test_named_profile_without_key_fails_closed(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={"status": "ok"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://gateway"
        )
        adapter = GatewayAdapter(
            "http://gateway",
            "global",
            {"default": "default-key"},
            default_profile="default",
            configured_profiles=["default", "named"],
            client=client,
        )
        with self.assertRaises(ValueError):
            await adapter.capabilities("named")
        self.assertFalse(requests)
        await adapter.capabilities("default")
        self.assertEqual("Bearer default-key", requests[0].headers["authorization"])
        await client.aclose()

    async def test_single_named_profile_never_reuses_default_key(self):
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"status": "ok"})
            ),
            base_url="http://gateway",
        )
        adapter = GatewayAdapter(
            "http://gateway",
            "default-key",
            default_profile="default",
            configured_profiles=["architect"],
            client=client,
        )

        with self.assertRaises(ValueError):
            await adapter.capabilities("architect")

        await client.aclose()

    async def test_discover_profiles_filters_blanks_and_stop_uses_profile_key(self):
        seen = []

        def handler(request):
            seen.append(request)
            if request.url.path == "/api/profiles":
                return httpx.Response(
                    200,
                    json={"profiles": ["", {"name": "architect"}, {"profile": ""}]},
                )
            return httpx.Response(200, json={"status": "stopping"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://gateway"
        )
        adapter = GatewayAdapter(
            "http://gateway",
            "default-key",
            {"architect": "architect-key"},
            client=client,
        )
        self.assertEqual(["architect"], await adapter.discover_profiles())
        await adapter.stop_run("architect", "run/1")
        self.assertEqual(
            "/p/architect/v1/runs/run%2F1/stop", urlsplit(str(seen[-1].url)).path
        )
        self.assertEqual("Bearer architect-key", seen[-1].headers["authorization"])
        await client.aclose()


if __name__ == "__main__":
    unittest.main()
