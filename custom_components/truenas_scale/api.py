"""TrueNAS Scale 25.10 / 26 JSON-RPC 2.0 over WebSocket API Client."""

import asyncio
import json
import logging
import ssl
from typing import Any, Callable, Dict, List, Optional, Union

import aiohttp

_LOGGER = logging.getLogger(__name__)


def _sanitize_host_and_port(host: str, port: int) -> tuple[str, int]:
    """Clean host string to remove protocol, path, and port if embedded."""
    cleaned = host.strip()
    for prefix in ("https://", "http://", "wss://", "ws://"):
        if cleaned.lower().startswith(prefix):
            cleaned = cleaned[len(prefix):]
    cleaned = cleaned.split("/")[0]
    if ":" in cleaned:
        parts = cleaned.split(":")
        cleaned = parts[0]
        try:
            port = int(parts[1])
        except ValueError:
            pass
    return cleaned, port


class TrueNASAPIError(Exception):
    """General TrueNAS API error."""


class TrueNASAuthError(TrueNASAPIError):
    """Authentication error."""


class TrueNASConnectionError(TrueNASAPIError):
    """Connection error."""


class TrueNASScaleClient:
    """Async WebSocket client for TrueNAS Scale 25.10 / 26 JSON-RPC 2.0 API."""

    def __init__(
        self,
        host: str,
        api_key: str,
        username: str = "root",
        port: int = 443,
        use_ssl: bool = True,
        verify_ssl: bool = False,
        path: str = "/api/current",
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        clean_host, clean_port = _sanitize_host_and_port(host, port)
        self.host = clean_host
        self.api_key = api_key
        self.username = username or "root"
        self.port = clean_port
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl
        self.path = path
        self._session = session
        self._own_session = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._req_id = 1
        self._pending_requests: Dict[Union[int, str], asyncio.Future] = {}
        self._listen_task: Optional[asyncio.Task] = None
        self._event_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self._realtime_data: Dict[str, Any] = {}
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Return True if currently connected to the TrueNAS WebSocket API."""
        return self._connected

    @property
    def ws_url(self) -> str:
        """Construct WebSocket URL."""
        protocol = "wss" if self.use_ssl else "ws"
        endpoint_path = self.path if self.path.startswith("/") else f"/{self.path}"
        return f"{protocol}://{self.host}:{self.port}{endpoint_path}"

    async def connect(self) -> bool:
        """Connect to TrueNAS WebSocket endpoint and authenticate."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._own_session = True

        ssl_context = None
        if self.use_ssl:
            loop = asyncio.get_running_loop()

            def _create_ssl_context() -> ssl.SSLContext:
                ctx = ssl.create_default_context()
                if not self.verify_ssl:
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                return ctx

            ssl_context = await loop.run_in_executor(None, _create_ssl_context)

        # Candidate WebSocket paths to attempt
        candidate_paths = [self.path, "/api/current", "/api/v2.0", "/websocket"]
        unique_paths = []
        for p in candidate_paths:
            if p not in unique_paths:
                unique_paths.append(p)

        last_err = None
        for p in unique_paths:
            self.path = p
            _LOGGER.debug("Attempting WebSocket connection to %s", self.ws_url)
            try:
                self._ws = await self._session.ws_connect(
                    self.ws_url,
                    ssl=ssl_context if self.use_ssl else False,
                    autoping=True,
                    heartbeat=30.0,
                    timeout=5.0,
                )
                self._connected = True
                self._listen_task = asyncio.create_task(self._listen_loop())

                # Perform login
                await self.login()
                _LOGGER.info("Connected and authenticated to TrueNAS Scale at %s", self.ws_url)
                return True
            except TrueNASAuthError as err:
                self._connected = False
                await self.close()
                _LOGGER.warning("Authentication failed on %s: %s", self.ws_url, err)
                raise
            except Exception as err:
                self._connected = False
                self._fail_pending_requests(
                    TrueNASConnectionError(f"Connection failed: {err}")
                )
                if self._listen_task and not self._listen_task.done():
                    self._listen_task.cancel()
                if self._ws and not self._ws.closed:
                    await self._ws.close()
                last_err = err
                _LOGGER.debug("Failed connecting to %s: %s", self.ws_url, err)

        raise TrueNASConnectionError(f"Could not connect to TrueNAS Scale on any path: {last_err}") from last_err

    async def close(self) -> None:
        """Close WebSocket connection."""
        self._connected = False
        self._fail_pending_requests(TrueNASConnectionError("Connection closed by client"))
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        if self._own_session and self._session:
            await self._session.close()

    async def login(self) -> bool:
        """Authenticate using API Key via JSON-RPC 2.0 (auth.login_ex or auth.login_with_api_key)."""
        authenticated = False
        # 1. Primary method: auth.login_ex with API_KEY_PLAIN and username (TrueNAS 25.10+ standard)
        try:
            res = await self.call(
                "auth.login_ex",
                [{"mechanism": "API_KEY_PLAIN", "username": self.username, "api_key": self.api_key}],
                timeout=5.0,
            )
            if res is True or (isinstance(res, dict) and res.get("response_type") in ("SUCCESS", "CONNECTED")):
                _LOGGER.info("Successfully authenticated using auth.login_ex (with username)")
                authenticated = True
            elif isinstance(res, dict) and res.get("response_type") == "FAILED":
                raise TrueNASAuthError(f"Authentication failed: {res.get('error', 'Invalid API Key')}")
        except TrueNASAuthError:
            raise
        except Exception as err:
            _LOGGER.debug("auth.login_ex (with username) failed, trying variants: %s", err)
            err_str = str(err).lower()
            if ("invalid key" in err_str or "auth" in err_str or "denied" in err_str) and "-32602" not in err_str and "-32600" not in err_str:
                raise TrueNASAuthError(f"Authentication failed: {err}") from err

        if not authenticated:
            # 2. Try auth.login_ex with API_KEY_PLAIN without username
            try:
                res = await self.call(
                    "auth.login_ex",
                    [{"mechanism": "API_KEY_PLAIN", "api_key": self.api_key}],
                    timeout=5.0,
                )
                if res is True or (isinstance(res, dict) and res.get("response_type") in ("SUCCESS", "CONNECTED")):
                    _LOGGER.info("Successfully authenticated using auth.login_ex (without username)")
                    authenticated = True
                elif isinstance(res, dict) and res.get("response_type") == "FAILED":
                    raise TrueNASAuthError(f"Authentication failed: {res.get('error', 'Invalid API Key')}")
            except TrueNASAuthError:
                raise
            except Exception as err:
                _LOGGER.debug("auth.login_ex (without username) failed, trying fallback: %s", err)
                err_str = str(err).lower()
                if ("invalid key" in err_str or "auth" in err_str or "denied" in err_str) and "-32602" not in err_str and "-32600" not in err_str:
                    raise TrueNASAuthError(f"Authentication failed: {err}") from err

        if not authenticated:
            # 3. Fallback method: auth.login_with_api_key (for TrueNAS versions < 25.10)
            try:
                res = await self.call("auth.login_with_api_key", [self.api_key], timeout=5.0)
                if res is True or (isinstance(res, dict) and res.get("response_type") in ("SUCCESS", "CONNECTED")):
                    _LOGGER.info("Authenticated using auth.login_with_api_key fallback")
                    authenticated = True
                elif isinstance(res, dict) and res.get("response_type") == "FAILED":
                    raise TrueNASAuthError(f"Authentication failed: {res.get('error', 'Invalid API Key')}")
            except TrueNASAuthError:
                raise
            except Exception as err:
                _LOGGER.debug("auth.login_with_api_key failed: %s", err)

        if not authenticated:
            raise TrueNASAuthError("Authentication failed with provided API key")

        # Subscribe to real-time events upon successful login
        for sub_topic in ("reporting.realtime", "reporting.realtime:{}", 'reporting.realtime:{"interval": 2}'):
            try:
                await self.call("core.subscribe", [sub_topic])
                _LOGGER.info("Subscribed to %s", sub_topic)
            except Exception as err:
                _LOGGER.debug("Could not subscribe to %s: %s", sub_topic, err)

        return True

    async def call(
        self,
        method: str,
        params: Optional[Union[List[Any], Dict[str, Any]]] = None,
        timeout: float = 15.0,
    ) -> Any:
        """Execute a JSON-RPC 2.0 method call."""
        if not self._connected or not self._ws or self._ws.closed:
            if method.startswith("auth."):
                raise TrueNASConnectionError("Not connected to WebSocket during authentication")
            await self.connect()

        req_id = self._req_id
        self._req_id += 1

        payload: Dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
        }
        if isinstance(params, list):
            payload["params"] = params
        elif isinstance(params, dict):
            payload["params"] = [params]
        else:
            payload["params"] = []

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_requests[req_id] = future

        try:
            await self._ws.send_json(payload)
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self._pending_requests.pop(req_id, None)
            raise TrueNASAPIError(f"Timeout calling TrueNAS method '{method}'")
        except Exception as err:
            self._pending_requests.pop(req_id, None)
            raise TrueNASAPIError(f"Error calling TrueNAS method '{method}': {err}") from err

    async def subscribe(self, collection: str = "*") -> Any:
        """Subscribe to real-time events."""
        return await self.call("core.subscribe", [collection])

    async def _listen_loop(self) -> None:
        """Background loop reading WebSocket messages."""
        while self._connected and self._ws and not self._ws.closed:
            try:
                msg = await self._ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    self._handle_message(data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    _LOGGER.warning("TrueNAS WebSocket connection closed or encountered an error")
                    self._connected = False
                    self._fail_pending_requests(TrueNASConnectionError("WebSocket connection lost"))
                    break
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Error in TrueNAS WebSocket listen loop: %s", err)

    def _fail_pending_requests(self, exc: Exception) -> None:
        """Reject all pending requests with an exception."""
        pending = list(self._pending_requests.items())
        self._pending_requests.clear()
        for _, future in pending:
            if not future.done():
                future.set_exception(exc)

    def _handle_message(self, data: Dict[str, Any]) -> None:
        """Process incoming WebSocket JSON-RPC message or notification."""
        raw_id = data.get("id")
        if raw_id is not None:
            matched_key = None
            if raw_id in self._pending_requests:
                matched_key = raw_id
            elif isinstance(raw_id, str) and raw_id.isdigit() and int(raw_id) in self._pending_requests:
                matched_key = int(raw_id)
            elif isinstance(raw_id, int) and str(raw_id) in self._pending_requests:
                matched_key = str(raw_id)

            if matched_key is not None:
                future = self._pending_requests.pop(matched_key)
                if not future.done():
                    if "error" in data:
                        err_info = data["error"]
                        err_msg = err_info.get("message") if isinstance(err_info, dict) else str(err_info)
                        err_code = err_info.get("code") if isinstance(err_info, dict) else "N/A"
                        future.set_exception(
                            TrueNASAPIError(
                                f"JSON-RPC Error {err_code}: {err_msg}"
                            )
                        )
                    else:
                        future.set_result(data.get("result"))
                return

        # Catch all notifications & realtime telemetry updates
        msg_type = str(data.get("method") or data.get("collection") or data.get("msg") or "")
        params = data.get("params")
        if isinstance(params, list) and len(params) > 0:
            params = params[0]
        if not isinstance(params, dict):
            params = data.get("fields") if isinstance(data.get("fields"), dict) else data

        collection = str(params.get("collection") or params.get("name") or "") if isinstance(params, dict) else ""

        is_realtime = (
            "reporting.realtime" in msg_type
            or "reporting.realtime" in collection
        )

        if is_realtime:
            fields = params.get("fields") if isinstance(params, dict) and isinstance(params.get("fields"), dict) else params
            if isinstance(fields, dict):
                self._realtime_data.update(fields)
                _LOGGER.debug("Realtime telemetry updated: %s", list(fields.keys()))

        for callback in self._event_callbacks:
            try:
                callback(data if isinstance(data, dict) else {})
            except Exception as err:
                _LOGGER.error("Error in event callback: %s", err)

    def register_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for collection_update events."""
        self._event_callbacks.append(callback)

    async def async_get_realtime_cache(self) -> Dict[str, Any]:
        """Fetch cached realtime statistics."""
        return dict(self._realtime_data)

    async def async_get_interfaces(self) -> List[Dict[str, Any]]:
        """Fetch network interfaces."""
        return await self.call("interface.query")

    async def async_get_system_info(self) -> Dict[str, Any]:
        """Fetch system information."""
        return await self.call("system.info")

    async def async_get_pools(self) -> List[Dict[str, Any]]:
        """Fetch ZFS pool information."""
        return await self.call("pool.query")

    async def async_get_datasets(self) -> List[Dict[str, Any]]:
        """Fetch datasets information."""
        try:
            return await self.call("pool.dataset.query")
        except Exception:
            return await self.call("dataset.query")

    async def async_get_disks(self) -> List[Dict[str, Any]]:
        """Fetch disk information."""
        return await self.call("disk.query")

    async def async_get_disk_temperatures(self) -> Dict[str, Any]:
        """Fetch disk temperatures."""
        return await self.call("disk.temperatures")

    async def async_get_services(self) -> List[Dict[str, Any]]:
        """Fetch system services."""
        return await self.call("service.query")

    async def async_get_apps(self) -> List[Dict[str, Any]]:
        """Fetch installed applications/containers."""
        return await self.call("app.query")

    async def async_get_alerts(self) -> List[Dict[str, Any]]:
        """Fetch active system alerts."""
        return await self.call("alert.list")

    async def async_get_vms(self) -> List[Dict[str, Any]]:
        """Fetch Virtual Machines."""
        return await self.call("vm.query")

    async def async_upgrade_app(self, app_name: str, version: Optional[str] = None) -> Any:
        """Submit once, then check the background job through completion."""
        options = {"app_version": version or "latest"}
        job_id = await self.call("app.upgrade", [app_name, options])
        if not isinstance(job_id, int) or isinstance(job_id, bool):
            raise TrueNASAPIError(f"Unexpected upgrade job response for {app_name}: {job_id!r}")
        deadline = asyncio.get_running_loop().time() + 1800
        while asyncio.get_running_loop().time() < deadline:
            jobs = await self.call("core.get_jobs", [[["id", "=", job_id]]])
            if not jobs:
                raise TrueNASAPIError(f"Upgrade job {job_id} for {app_name} could not be found; check TrueNAS Jobs before retrying")
            job = jobs[0]
            state = job.get("state")
            if state == "SUCCESS":
                return job.get("result")
            if state in ("FAILED", "ABORTED"):
                raise TrueNASAPIError(f"Upgrade job {job_id} for {app_name} {state.lower()}: {job.get('error') or 'No error details returned'}")
            await asyncio.sleep(2)
        raise TrueNASAPIError(f"Timed out waiting for upgrade job {job_id} for {app_name}; it may still be running. Check TrueNAS Jobs before retrying")
