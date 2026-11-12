"""
Test Harness for Multi-Service Integration Testing.
Provides isolated, genuine live HTTP server instances on test ports
allowing end-to-end network verification without mocking.
"""
import http.server
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.api_gateway.main import GatewayRequestHandler
import services.api_gateway.main as gw_module
from services.backend_worker.main import WorkerRequestHandler
import services.backend_worker.main as worker_module
from services.data_cache_store.main import CacheRequestHandler, cache_store
import services.data_cache_store.main as cache_module
from services.frontend_service.main import FrontendRequestHandler
import services.frontend_service.main as frontend_module

TEST_PORTS = {
    "cache": 18002,
    "worker": 18001,
    "gateway": 18000,
    "frontend": 18080,
}

_servers: Dict[str, http.server.ThreadingHTTPServer] = {}
_threads: Dict[str, threading.Thread] = {}
_is_running = False
_lock = threading.Lock()


def start_test_cluster() -> Dict[str, str]:
    """Starts all 4 microservices on test ports with genuine TCP sockets."""
    global _is_running
    with _lock:
        if _is_running:
            return get_cluster_urls()

        # Configure cross-service URLs
        cache_url = f"http://127.0.0.1:{TEST_PORTS['cache']}"
        worker_url = f"http://127.0.0.1:{TEST_PORTS['worker']}"
        gateway_url = f"http://127.0.0.1:{TEST_PORTS['gateway']}"

        worker_module.CACHE_STORE_URL = cache_url
        gw_module.WORKER_SERVICE_URL = worker_url
        gw_module.CACHE_SERVICE_URL = cache_url
        frontend_module.GATEWAY_URL = gateway_url

        # Create servers
        _servers["cache"] = http.server.ThreadingHTTPServer(("127.0.0.1", TEST_PORTS["cache"]), CacheRequestHandler)
        _servers["worker"] = http.server.ThreadingHTTPServer(("127.0.0.1", TEST_PORTS["worker"]), WorkerRequestHandler)
        _servers["gateway"] = http.server.ThreadingHTTPServer(("127.0.0.1", TEST_PORTS["gateway"]), GatewayRequestHandler)
        _servers["frontend"] = http.server.ThreadingHTTPServer(("127.0.0.1", TEST_PORTS["frontend"]), FrontendRequestHandler)

        for name, srv in _servers.items():
            t = threading.Thread(target=srv.serve_forever, daemon=True, name=f"test-server-{name}")
            _threads[name] = t
            t.start()

        # Wait for all healthchecks to respond
        deadline = time.time() + 5.0
        for name, port in TEST_PORTS.items():
            url = f"http://127.0.0.1:{port}/health"
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(url, timeout=0.5) as resp:
                        if resp.status == 200:
                            break
                except Exception:
                    time.sleep(0.05)

        _is_running = True
        return get_cluster_urls()


def stop_test_cluster() -> None:
    """Stops all running test microservice servers."""
    global _is_running
    with _lock:
        if not _is_running:
            return
        for srv in _servers.values():
            try:
                srv.shutdown()
                srv.server_close()
            except Exception:
                pass
        _servers.clear()
        _threads.clear()
        _is_running = False


def get_cluster_urls() -> Dict[str, str]:
    return {
        "cache": f"http://127.0.0.1:{TEST_PORTS['cache']}",
        "worker": f"http://127.0.0.1:{TEST_PORTS['worker']}",
        "gateway": f"http://127.0.0.1:{TEST_PORTS['gateway']}",
        "frontend": f"http://127.0.0.1:{TEST_PORTS['frontend']}",
    }


def http_request(method: str, url: str, payload: Optional[Dict[str, Any]] = None, timeout: float = 3.0) -> Tuple[int, Dict[str, Any]]:
    """Helper to perform HTTP requests in test suite with finite timeout."""
    req_headers = {"Content-Type": "application/json", "Accept": "application/json"}
    data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method.upper())

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except Exception:
                return resp.status, {"raw": body}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, {"raw": body}
