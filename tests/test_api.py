"""Tests for TrueNAS Scale API Client."""

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp

from custom_components.truenas_scale.api import (
    TrueNASAPIError,
    TrueNASAuthError,
    TrueNASConnectionError,
    TrueNASScaleClient,
    _sanitize_host_and_port,
)


class TestTrueNASAPI(unittest.IsolatedAsyncioTestCase):
    """Test API client."""

    async def test_client_init(self):
        client = TrueNASScaleClient(
            host="192.168.1.100",
            api_key="test_api_key",
            username="root",
            port=443,
            use_ssl=True,
            verify_ssl=False,
        )
        self.assertEqual(client.host, "192.168.1.100")
        self.assertEqual(client.api_key, "test_api_key")
        self.assertEqual(client.port, 443)
        self.assertFalse(client.is_connected)

    async def test_client_connect_ssl_context(self):
        session = MagicMock()
        ws_mock = AsyncMock()
        session.ws_connect = AsyncMock(return_value=ws_mock)

        client = TrueNASScaleClient("192.168.1.100", "key", use_ssl=True, verify_ssl=False, session=session)

        # Mock authentication call
        with patch.object(client, "call", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = True
            await client.connect()
            self.assertTrue(client.is_connected)

    async def test_client_close(self):
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._ws = ws_mock
        client._connected = True

        await client.close()
        self.assertFalse(client.is_connected)
        self.assertIsNone(client._ws)

    async def test_client_handle_message_string_id(self):
        client = TrueNASScaleClient("192.168.1.100", "test_key")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        client._pending_requests[1] = future

        # String ID "1" matching int 1
        data = {"jsonrpc": "2.0", "id": "1", "result": {"success": True}}
        client._handle_message(data)
        self.assertTrue(future.done())
        res = await future
        self.assertEqual(res, {"success": True})

    async def test_client_handle_message_error(self):
        client = TrueNASScaleClient("192.168.1.100", "test_key")
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        client._pending_requests[2] = future

        data = {"jsonrpc": "2.0", "id": 2, "error": {"code": -32601, "message": "Method not found"}}
        client._handle_message(data)
        self.assertTrue(future.done())
        with self.assertRaises(TrueNASAPIError):
            await future

    async def test_client_handle_realtime_event(self):
        client = TrueNASScaleClient("192.168.1.100", "test_key")
        callback = MagicMock()
        client.register_callback(callback)

        event_data = {
            "method": "reporting.realtime",
            "params": {
                "fields": {
                    "cpu": {"usage": 45.0, "temp": 50.0},
                }
            },
        }
        client._handle_message(event_data)
        realtime_cache = await client.async_get_realtime_cache()
        self.assertIn("cpu", realtime_cache)
        self.assertEqual(realtime_cache["cpu"]["usage"], 45.0)
        self.assertTrue(callback.called)

    async def test_client_api_methods(self):
        client = TrueNASScaleClient("192.168.1.100", "test_key")
        client.call = AsyncMock(return_value=[])

        await client.async_get_system_info()
        client.call.assert_called_with("system.info")

        await client.async_get_pools()
        client.call.assert_called_with("pool.query")

        await client.async_get_disks()
        client.call.assert_called_with("disk.query")

        await client.async_get_disk_temperatures()
        client.call.assert_called_with("disk.temperatures")

        await client.async_get_services()
        client.call.assert_called_with("service.query")

        await client.async_get_apps()
        client.call.assert_called_with("app.query")

        await client.async_get_alerts()
        client.call.assert_called_with("alert.list")

        await client.async_get_vms()
        client.call.assert_called_with("vm.query")

    # -- connect() -----------------------------------------------------

    async def test_connect_creates_own_session_when_none_provided(self):
        ws_mock = AsyncMock()
        ws_mock.closed = False
        # A real listen task is spawned on successful connect(); give it a
        # message that ends the loop immediately instead of spinning on an
        # unconfigured AsyncMock.receive() forever.
        ws_mock.receive = AsyncMock(
            return_value=SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        )
        session_instance = MagicMock()
        session_instance.ws_connect = AsyncMock(return_value=ws_mock)
        session_cls = MagicMock(return_value=session_instance)

        with patch("custom_components.truenas_scale.api.aiohttp.ClientSession", session_cls):
            client = TrueNASScaleClient("192.168.1.100", "key", use_ssl=False)
            with patch.object(client, "login", new_callable=AsyncMock, return_value=True):
                result = await client.connect()

        self.assertTrue(result)
        self.assertTrue(client._own_session)
        self.assertIs(client._session, session_instance)

    async def test_connect_ssl_disabled_skips_context_creation(self):
        session = MagicMock()
        ws_mock = AsyncMock()
        ws_mock.closed = False
        ws_mock.receive = AsyncMock(
            return_value=SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        )
        session.ws_connect = AsyncMock(return_value=ws_mock)
        client = TrueNASScaleClient("192.168.1.100", "key", use_ssl=False, session=session)

        with patch.object(client, "login", new_callable=AsyncMock, return_value=True):
            result = await client.connect()

        self.assertTrue(result)
        self.assertEqual(session.ws_connect.call_args.kwargs["ssl"], False)

    async def test_connect_auth_error_closes_and_reraises(self):
        session = MagicMock()
        ws_mock = AsyncMock()
        ws_mock.closed = False
        ws_mock.receive = AsyncMock(
            return_value=SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        )
        session.ws_connect = AsyncMock(return_value=ws_mock)
        client = TrueNASScaleClient("192.168.1.100", "key", use_ssl=False, session=session)

        with patch.object(
            client, "login", new_callable=AsyncMock, side_effect=TrueNASAuthError("bad key")
        ):
            with self.assertRaises(TrueNASAuthError):
                await client.connect()

        self.assertFalse(client.is_connected)

    async def test_connect_generic_error_exhausts_all_paths(self):
        session = MagicMock()
        ws_mock = AsyncMock()
        ws_mock.closed = False
        ws_mock.receive = AsyncMock(
            return_value=SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        )
        session.ws_connect = AsyncMock(return_value=ws_mock)
        client = TrueNASScaleClient("192.168.1.100", "key", use_ssl=False, session=session)

        stale = asyncio.get_running_loop().create_future()
        client._pending_requests["stale"] = stale

        with patch.object(
            client, "login", new_callable=AsyncMock, side_effect=RuntimeError("boom")
        ):
            with self.assertRaises(TrueNASConnectionError):
                await client.connect()

        self.assertFalse(client.is_connected)
        # /api/current, /api/v2.0, /websocket - deduplicated candidate paths
        self.assertEqual(session.ws_connect.call_count, 3)
        ws_mock.close.assert_awaited()
        self.assertTrue(stale.done())

    # -- close() ---------------------------------------------------------

    async def test_close_cancels_listen_task_and_own_session(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock
        client._own_session = True
        session_mock = MagicMock()
        session_mock.close = AsyncMock()
        client._session = session_mock

        pending = asyncio.get_running_loop().create_future()
        client._pending_requests["req1"] = pending

        listen_task = asyncio.create_task(asyncio.sleep(10))
        client._listen_task = listen_task

        await client.close()
        await asyncio.sleep(0)

        self.assertFalse(client.is_connected)
        self.assertIsNone(client._ws)
        self.assertTrue(listen_task.cancelled())
        self.assertTrue(pending.done())
        with self.assertRaises(TrueNASConnectionError):
            pending.result()
        session_mock.close.assert_awaited_once()

    # -- login() -----------------------------------------------------------

    async def test_login_success_with_username_and_subscribe_failure_logged(self):
        client = TrueNASScaleClient("192.168.1.100", "key", username="admin")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex":
                return True
            if method == "core.subscribe":
                if params == ["reporting.realtime"]:
                    raise RuntimeError("subscribe failed")
                return True
            raise AssertionError(f"unexpected method {method}")

        client.call = AsyncMock(side_effect=fake_call)
        result = await client.login()
        self.assertTrue(result)

        auth_calls = [c for c in client.call.call_args_list if c.args[0] == "auth.login_ex"]
        self.assertEqual(len(auth_calls), 1)

    async def test_login_first_attempt_failed_response_raises_autherror(self):
        client = TrueNASScaleClient("192.168.1.100", "key", username="admin")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex" and params and "username" in params[0]:
                return {"response_type": "FAILED", "error": "bad key"}
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    async def test_login_first_attempt_exception_matching_auth_raises(self):
        client = TrueNASScaleClient("192.168.1.100", "key", username="admin")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex" and params and "username" in params[0]:
                raise RuntimeError("Access denied for this API key")
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    async def test_login_falls_back_to_no_username_variant(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        call_log = []

        async def fake_call(method, params=None, timeout=15.0):
            call_log.append((method, params))
            if method == "auth.login_ex" and params and "username" in params[0]:
                raise RuntimeError("Method not found (-32601)")
            if method == "auth.login_ex":
                return True
            if method == "core.subscribe":
                return True
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        result = await client.login()
        self.assertTrue(result)

        login_ex_calls = [p for m, p in call_log if m == "auth.login_ex"]
        self.assertEqual(len(login_ex_calls), 2)

    async def test_login_second_attempt_failed_response_raises_autherror(self):
        client = TrueNASScaleClient("192.168.1.100", "key")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex" and params and "username" in params[0]:
                raise RuntimeError("Method not found (-32601)")
            if method == "auth.login_ex":
                return {"response_type": "FAILED", "error": "bad key"}
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    async def test_login_second_attempt_exception_matching_auth_raises(self):
        client = TrueNASScaleClient("192.168.1.100", "key")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex" and params and "username" in params[0]:
                raise RuntimeError("Method not found (-32601)")
            if method == "auth.login_ex":
                raise RuntimeError("Invalid key supplied")
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    async def test_login_falls_back_to_legacy_api_key_method(self):
        client = TrueNASScaleClient("192.168.1.100", "key")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex":
                raise RuntimeError("Method not found (-32601)")
            if method == "auth.login_with_api_key":
                return True
            if method == "core.subscribe":
                return True
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        result = await client.login()
        self.assertTrue(result)

    async def test_login_legacy_method_failed_response_raises(self):
        client = TrueNASScaleClient("192.168.1.100", "key")

        async def fake_call(method, params=None, timeout=15.0):
            if method == "auth.login_ex":
                raise RuntimeError("Method not found (-32601)")
            if method == "auth.login_with_api_key":
                return {"response_type": "FAILED", "error": "nope"}
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    async def test_login_all_methods_fail_raises_autherror(self):
        client = TrueNASScaleClient("192.168.1.100", "key")

        async def fake_call(method, params=None, timeout=15.0):
            if method in ("auth.login_ex", "auth.login_with_api_key"):
                raise RuntimeError("Method not found (-32601)")
            raise AssertionError(method)

        client.call = AsyncMock(side_effect=fake_call)
        with self.assertRaises(TrueNASAuthError):
            await client.login()

    # -- call() ------------------------------------------------------------

    async def test_call_auth_method_when_not_connected_raises_connection_error(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        with self.assertRaises(TrueNASConnectionError):
            await client.call("auth.login_ex", [{}])

    async def test_call_not_connected_triggers_reconnect(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        ws_mock = AsyncMock()
        ws_mock.closed = False

        async def fake_connect():
            client._connected = True
            client._ws = ws_mock

        client.connect = AsyncMock(side_effect=fake_connect)

        async def fake_send_json(payload):
            client._handle_message({"jsonrpc": "2.0", "id": payload["id"], "result": "ok"})

        ws_mock.send_json = AsyncMock(side_effect=fake_send_json)

        result = await client.call("system.info")
        self.assertEqual(result, "ok")
        self.assertEqual(client._pending_requests, {})
        client.connect.assert_called_once()

    async def test_call_success_and_params_variants(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock
        sent_payloads = []

        async def fake_send_json(payload):
            sent_payloads.append(payload)
            # Mirror the real flow: the server's response arrives via the
            # listen loop and _handle_message(), which is what actually
            # pops the pending request - call() itself does not.
            client._handle_message({"jsonrpc": "2.0", "id": payload["id"], "result": None})

        ws_mock.send_json = AsyncMock(side_effect=fake_send_json)

        await client.call("m1", None)
        await client.call("m2", ["a", "b"])
        await client.call("m3", {"x": 1})

        self.assertEqual(sent_payloads[0]["params"], [])
        self.assertEqual(sent_payloads[1]["params"], ["a", "b"])
        self.assertEqual(sent_payloads[2]["params"], [{"x": 1}])
        self.assertEqual(client._pending_requests, {})

    async def test_call_timeout_raises_api_error(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        ws_mock.send_json = AsyncMock()  # never resolves the pending future
        client._ws = ws_mock

        with self.assertRaises(TrueNASAPIError):
            await client.call("slow.method", timeout=0.01)
        self.assertEqual(client._pending_requests, {})

    async def test_call_send_json_error_wraps_in_api_error(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        ws_mock.send_json = AsyncMock(side_effect=RuntimeError("socket broke"))
        client._ws = ws_mock

        with self.assertRaises(TrueNASAPIError):
            await client.call("some.method")
        self.assertEqual(client._pending_requests, {})

    async def test_subscribe_calls_core_subscribe(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client.call = AsyncMock(return_value=True)
        result = await client.subscribe("my.collection")
        client.call.assert_called_once_with("core.subscribe", ["my.collection"])
        self.assertTrue(result)

    # -- _listen_loop() ------------------------------------------------

    async def test_listen_loop_processes_text_message_then_stops_on_closed(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock

        future = asyncio.get_running_loop().create_future()
        client._pending_requests[7] = future

        text_msg = SimpleNamespace(
            type=aiohttp.WSMsgType.TEXT,
            data=json.dumps({"jsonrpc": "2.0", "id": 7, "result": "ok"}),
        )
        closed_msg = SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        ws_mock.receive = AsyncMock(side_effect=[text_msg, closed_msg])

        await client._listen_loop()

        self.assertEqual(await future, "ok")
        self.assertFalse(client.is_connected)

    async def test_listen_loop_error_message_sets_disconnected(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock

        pending = asyncio.get_running_loop().create_future()
        client._pending_requests[1] = pending

        error_msg = SimpleNamespace(type=aiohttp.WSMsgType.ERROR, data=None)
        ws_mock.receive = AsyncMock(return_value=error_msg)

        await client._listen_loop()

        self.assertFalse(client.is_connected)
        self.assertTrue(pending.done())
        with self.assertRaises(TrueNASConnectionError):
            pending.result()

    async def test_listen_loop_swallows_cancelled_error(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock
        ws_mock.receive = AsyncMock(side_effect=asyncio.CancelledError())

        # Should return normally: the loop catches CancelledError and breaks
        # instead of propagating it.
        await client._listen_loop()

    async def test_listen_loop_logs_and_continues_on_malformed_message(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client._connected = True
        ws_mock = AsyncMock()
        ws_mock.closed = False
        client._ws = ws_mock

        bad_msg = SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data="not-json")
        closed_msg = SimpleNamespace(type=aiohttp.WSMsgType.CLOSED, data=None)
        ws_mock.receive = AsyncMock(side_effect=[bad_msg, closed_msg])

        await client._listen_loop()
        self.assertFalse(client.is_connected)

    # -- _fail_pending_requests() ---------------------------------------

    async def test_fail_pending_requests_skips_already_done_futures(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        loop = asyncio.get_running_loop()
        done_future = loop.create_future()
        done_future.set_result("already resolved")
        pending_future = loop.create_future()
        client._pending_requests = {"done": done_future, "pending": pending_future}

        client._fail_pending_requests(TrueNASAPIError("failure"))

        self.assertEqual(done_future.result(), "already resolved")
        self.assertTrue(pending_future.done())
        with self.assertRaises(TrueNASAPIError):
            pending_future.result()
        self.assertEqual(client._pending_requests, {})

    # -- _handle_message() ------------------------------------------------

    async def test_handle_message_int_id_matches_string_key(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        future = asyncio.get_running_loop().create_future()
        client._pending_requests["5"] = future

        client._handle_message({"jsonrpc": "2.0", "id": 5, "result": "done"})

        self.assertTrue(future.done())
        self.assertEqual(await future, "done")

    async def test_handle_message_unwraps_list_params(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        data = {
            "method": "reporting.realtime",
            "params": [{"fields": {"cpu": {"usage": 12.5}}}],
        }
        client._handle_message(data)
        self.assertEqual(client._realtime_data["cpu"]["usage"], 12.5)

    async def test_handle_message_falls_back_to_fields_when_no_params(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        data = {"method": "reporting.realtime", "fields": {"mem": {"usage": 33}}}
        client._handle_message(data)
        self.assertEqual(client._realtime_data["mem"]["usage"], 33)

    async def test_handle_message_with_unmatched_id_does_not_raise(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        # No pending request registered for id 99 - should be treated as a
        # harmless notification instead of raising.
        client._handle_message({"jsonrpc": "2.0", "id": 99, "result": "orphan"})

    async def test_handle_message_callback_exception_is_logged_not_raised(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        bad_callback = MagicMock(side_effect=RuntimeError("callback exploded"))
        client.register_callback(bad_callback)

        client._handle_message({"method": "some.notification", "params": {}})

        self.assertTrue(bad_callback.called)

    # -- simple passthrough / fallback methods --------------------------

    async def test_async_get_interfaces(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client.call = AsyncMock(return_value=[{"name": "eth0"}])
        result = await client.async_get_interfaces()
        client.call.assert_called_once_with("interface.query")
        self.assertEqual(result, [{"name": "eth0"}])

    async def test_async_get_datasets_primary_success(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client.call = AsyncMock(return_value=[{"id": "pool/ds"}])
        result = await client.async_get_datasets()
        client.call.assert_called_once_with("pool.dataset.query")
        self.assertEqual(result, [{"id": "pool/ds"}])

    async def test_async_get_datasets_falls_back_on_error(self):
        client = TrueNASScaleClient("192.168.1.100", "key")
        client.call = AsyncMock(side_effect=[TrueNASAPIError("nope"), [{"id": "legacy"}]])
        result = await client.async_get_datasets()
        self.assertEqual(result, [{"id": "legacy"}])
        self.assertEqual(
            [c.args[0] for c in client.call.call_args_list],
            ["pool.dataset.query", "dataset.query"],
        )

    async def test_async_upgrade_app_waits_for_success(self):
        client = TrueNASScaleClient("example.test", "key")
        client.call = AsyncMock(side_effect=[17, [{"state": "WAITING"}], [{"state": "RUNNING"}], [{"state": "SUCCESS", "result": {"version": "2"}}]])
        with patch("custom_components.truenas_scale.api.asyncio.sleep", new_callable=AsyncMock):
            result = await client.async_upgrade_app("plex")
        self.assertEqual(result, {"version": "2"})
        self.assertEqual(client.call.call_args_list[0], call("app.upgrade", ["plex", {"app_version": "latest"}]))
        self.assertEqual(client.call.call_args_list[1], call("core.get_jobs", [[["id", "=", 17]]]))

    async def test_async_upgrade_app_passes_requested_version(self):
        client = TrueNASScaleClient("example.test", "key")
        client.call = AsyncMock(side_effect=[17, [{"state": "SUCCESS"}]])
        await client.async_upgrade_app("plex", "2.0")
        self.assertEqual(client.call.call_args_list[0], call("app.upgrade", ["plex", {"app_version": "2.0"}]))

    async def test_async_upgrade_app_surfaces_terminal_job_errors(self):
        for state in ("FAILED", "ABORTED"):
            with self.subTest(state=state):
                client = TrueNASScaleClient("example.test", "key")
                client.call = AsyncMock(side_effect=[17, [{"state": state, "error": "Validation failed"}]])
                with self.assertRaisesRegex(TrueNASAPIError, "Validation failed"):
                    await client.async_upgrade_app("plex")
                self.assertEqual(client.call.await_count, 2)

    async def test_async_upgrade_app_does_not_resubmit_on_error(self):
        client = TrueNASScaleClient("example.test", "key")
        client.call = AsyncMock(side_effect=TrueNASAPIError("Permission denied"))
        with self.assertRaisesRegex(TrueNASAPIError, "Permission denied"):
            await client.async_upgrade_app("plex")
        client.call.assert_awaited_once()

    async def test_async_upgrade_app_rejects_invalid_job_id(self):
        for response in (True, None, {"job_id": 17}):
            with self.subTest(response=response):
                client = TrueNASScaleClient("example.test", "key")
                client.call = AsyncMock(return_value=response)
                with self.assertRaisesRegex(TrueNASAPIError, "Unexpected upgrade job response"):
                    await client.async_upgrade_app("plex")

    async def test_async_upgrade_app_missing_job_warns_before_retry(self):
        client = TrueNASScaleClient("example.test", "key")
        client.call = AsyncMock(side_effect=[17, []])
        with self.assertRaisesRegex(TrueNASAPIError, "check TrueNAS Jobs before retrying"):
            await client.async_upgrade_app("plex")


class TestSanitizeHostAndPort(unittest.TestCase):
    """Test the _sanitize_host_and_port helper."""

    def test_strips_protocol_and_path(self):
        host, port = _sanitize_host_and_port("https://192.168.1.50/api/current", 443)
        self.assertEqual(host, "192.168.1.50")
        self.assertEqual(port, 443)

    def test_extracts_embedded_port(self):
        host, port = _sanitize_host_and_port("192.168.1.50:8443", 443)
        self.assertEqual(host, "192.168.1.50")
        self.assertEqual(port, 8443)

    def test_invalid_embedded_port_keeps_default(self):
        host, port = _sanitize_host_and_port("192.168.1.50:notanumber", 443)
        self.assertEqual(host, "192.168.1.50")
        self.assertEqual(port, 443)

    def test_strips_whitespace_protocol_port_and_path(self):
        host, port = _sanitize_host_and_port("  wss://truenas.local:9000/foo/bar  ", 443)
        self.assertEqual(host, "truenas.local")
        self.assertEqual(port, 9000)
