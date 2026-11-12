"""
Service D: data-cache-store
High-performance, in-memory key-value cache store with TTL expiration,
background eviction, Prometheus observability, and structured REST API.
"""
import http.server
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

# Add workspace to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.common.config import get_env_int, get_env_str
from services.common.metrics import MetricsRegistry

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"data-cache-store","message":"%(message)s"}',
)
logger = logging.getLogger("data-cache-store")

SERVICE_NAME = "data-cache-store"
PORT = get_env_int("PORT", 8002)
HOST = get_env_str("HOST", "0.0.0.0")
DEFAULT_TTL = get_env_int("DEFAULT_TTL_SECONDS", 120)

# Observability Metrics Registry
metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter(
    "http_requests_total", "Total count of HTTP requests processed by data-cache-store"
)
HTTP_DURATION = metrics_reg.register_histogram(
    "http_request_duration_seconds", "HTTP request latency in seconds for data-cache-store"
)
CACHE_OPERATIONS = metrics_reg.register_counter(
    "cache_operations_total", "Total cache operations (get, set, delete)"
)
CACHE_HITS = metrics_reg.register_counter("cache_hits_total", "Total cache hit count")
CACHE_MISSES = metrics_reg.register_counter("cache_misses_total", "Total cache miss count")
CACHE_EVICTIONS = metrics_reg.register_counter("cache_evictions_total", "Total cache expired items evicted")
CACHE_KEYS_GAUGE = metrics_reg.register_gauge("cache_keys_total", "Current number of keys stored in cache")


class CacheItem:
    def __init__(self, key: str, value: Any, ttl_seconds: Optional[float] = None):
        self.key = key
        self.value = value
        self.created_at = time.time()
        self.expires_at = (self.created_at + ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return False
        return (now or time.time()) >= self.expires_at

    def ttl_remaining(self, now: Optional[float] = None) -> Optional[float]:
        if self.expires_at is None:
            return None
        rem = self.expires_at - (now or time.time())
        return max(0.0, round(rem, 2))


class CacheStorage:
    def __init__(self):
        self._store: Dict[str, CacheItem] = {}
        self._lock = threading.Lock()
        self._stop_eviction = threading.Event()
        self._eviction_thread = threading.Thread(target=self._eviction_loop, daemon=True)
        self._eviction_thread.start()

    def get(self, key: str) -> Optional[CacheItem]:
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            if item.is_expired():
                del self._store[key]
                CACHE_EVICTIONS.inc()
                CACHE_KEYS_GAUGE.set(len(self._store))
                return None
            return item

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> CacheItem:
        with self._lock:
            item = CacheItem(key, value, ttl_seconds)
            self._store[key] = item
            CACHE_KEYS_GAUGE.set(len(self._store))
            return item

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                CACHE_KEYS_GAUGE.set(len(self._store))
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
            CACHE_KEYS_GAUGE.set(0)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            active_keys = [k for k, item in self._store.items() if not item.is_expired(now)]
            return {
                "total_keys": len(active_keys),
                "keys": active_keys[:50],  # preview top 50
            }

    def _eviction_loop(self) -> None:
        """Background thread running periodic eviction of expired entries."""
        while not self._stop_eviction.wait(timeout=2.0):
            with self._lock:
                now = time.time()
                expired = [k for k, item in self._store.items() if item.is_expired(now)]
                for k in expired:
                    del self._store[k]
                    CACHE_EVICTIONS.inc()
                if expired:
                    CACHE_KEYS_GAUGE.set(len(self._store))


cache_store = CacheStorage()


class CacheRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        # Suppress default noisy stdio logging in favor of structured logger
        pass

    def _send_json(self, status: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_GET(self) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            if path == "/health":
                stats = cache_store.stats()
                self._send_json(200, {
                    "status": "healthy",
                    "service": SERVICE_NAME,
                    "active_keys": stats["total_keys"],
                    "uptime_seconds": round(time.time() - SERVER_START_TIME, 2),
                })
                return

            if path == "/metrics":
                exposition = metrics_reg.generate_exposition().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(exposition)))
                self.end_headers()
                self.wfile.write(exposition)
                return

            if path == "/cache":
                stats = cache_store.stats()
                self._send_json(200, {
                    "service": SERVICE_NAME,
                    "stats": stats,
                })
                return

            if path.startswith("/cache/"):
                key = path[len("/cache/"):]
                if not key:
                    status = 400
                    self._send_json(400, {"error": "Cache key cannot be empty", "service": SERVICE_NAME})
                    return

                CACHE_OPERATIONS.inc(labels={"operation": "get"})
                item = cache_store.get(key)
                if item is None:
                    CACHE_MISSES.inc()
                    status = 404
                    self._send_json(404, {
                        "status": "miss",
                        "error": f"Key '{key}' not found or expired",
                        "key": key,
                        "service": SERVICE_NAME,
                    })
                    return

                CACHE_HITS.inc()
                self._send_json(200, {
                    "status": "hit",
                    "key": item.key,
                    "value": item.value,
                    "ttl_remaining_seconds": item.ttl_remaining(),
                    "service": SERVICE_NAME,
                })
                return

            status = 404
            self._send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "GET"})

    def do_POST(self) -> None:
        self._handle_write("POST")

    def do_PUT(self) -> None:
        self._handle_write("PUT")

    def _handle_write(self, method: str) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > 1_000_000:  # 1MB safeguard
                status = 413
                self._send_json(413, {"error": "Payload exceeds maximum allowed limit (1MB)", "service": SERVICE_NAME})
                return

            raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
            try:
                payload = json.loads(raw_body)
            except json.JSONDecodeError as err:
                status = 400
                self._send_json(400, {"error": "Invalid JSON payload", "details": str(err), "service": SERVICE_NAME})
                return

            # Supports /cache or /cache/<key>
            key = None
            if path == "/cache":
                key = payload.get("key")
            elif path.startswith("/cache/"):
                key = path[len("/cache/"):]

            if not key or not isinstance(key, str) or not key.strip():
                status = 400
                self._send_json(400, {"error": "Valid 'key' string is required", "service": SERVICE_NAME})
                return

            if "value" not in payload:
                status = 400
                self._send_json(400, {"error": "Field 'value' is required", "service": SERVICE_NAME})
                return

            ttl = payload.get("ttl_seconds", DEFAULT_TTL)
            if ttl is not None:
                try:
                    ttl = float(ttl)
                except ValueError:
                    status = 400
                    self._send_json(400, {"error": "Field 'ttl_seconds' must be numeric", "service": SERVICE_NAME})
                    return

            CACHE_OPERATIONS.inc(labels={"operation": "set"})
            item = cache_store.set(key.strip(), payload["value"], ttl)
            status = 201 if method == "POST" else 200

            self._send_json(status, {
                "status": "stored",
                "key": item.key,
                "ttl_seconds": ttl,
                "service": SERVICE_NAME,
            })

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": method, "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": method})

    def do_DELETE(self) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            if path == "/cache":
                cache_store.clear()
                self._send_json(200, {"status": "cleared", "service": SERVICE_NAME})
                return

            if path.startswith("/cache/"):
                key = path[len("/cache/"):]
                CACHE_OPERATIONS.inc(labels={"operation": "delete"})
                deleted = cache_store.delete(key)
                if not deleted:
                    status = 404
                    self._send_json(404, {"error": f"Key '{key}' not found", "service": SERVICE_NAME})
                    return

                self._send_json(200, {"status": "deleted", "key": key, "service": SERVICE_NAME})
                return

            status = 404
            self._send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "DELETE", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "DELETE"})


SERVER_START_TIME = time.time()


def run_server(port: int = PORT, host: str = HOST) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer((host, port), CacheRequestHandler)
    logger.info("Service D [data-cache-store] listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    server = run_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down data-cache-store...")
        server.shutdown()
