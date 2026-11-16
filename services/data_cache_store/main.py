from collections import OrderedDict
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

from services.common.config import get_env_int, get_env_str
from services.common.http_server import (
    BoundedThreadingHTTPServer,
    JsonRequestHandler,
    configure_service_logger,
)
from services.common.metrics import MetricsRegistry

SERVICE_NAME = "data-cache-store"
logger = configure_service_logger(SERVICE_NAME)

PORT = get_env_int("PORT", 8002)
HOST = get_env_str("HOST", "0.0.0.0")
DEFAULT_TTL = get_env_int("DEFAULT_TTL_SECONDS", 120)
MAX_CACHE_ENTRIES = get_env_int("MAX_CACHE_ENTRIES", 10_000)

metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter("http_requests_total", "Total HTTP requests processed")
HTTP_DURATION = metrics_reg.register_histogram("http_request_duration_seconds", "HTTP request latency in seconds")
CACHE_OPERATIONS = metrics_reg.register_counter("cache_operations_total", "Total cache operations")
CACHE_HITS = metrics_reg.register_counter("cache_hits_total", "Total cache hits")
CACHE_MISSES = metrics_reg.register_counter("cache_misses_total", "Total cache misses")
CACHE_EVICTIONS = metrics_reg.register_counter("cache_evictions_total", "Total expired or capacity-evicted items")
CACHE_KEYS_GAUGE = metrics_reg.register_gauge("cache_keys_total", "Current number of cached keys")


class CacheItem:
    def __init__(self, key: str, value: Any, ttl_seconds: Optional[float] = None):
        self.key = key
        self.value = value
        now = time.monotonic()
        self.expires_at = (now + ttl_seconds) if ttl_seconds and ttl_seconds > 0 else None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return False
        current = time.monotonic() if now is None else now
        return current >= self.expires_at

    def ttl_remaining(self, now: Optional[float] = None) -> Optional[float]:
        if self.expires_at is None:
            return None
        current = time.monotonic() if now is None else now
        return max(0.0, round(self.expires_at - current, 2))


class CacheStorage:
    def __init__(self, max_entries: int = MAX_CACHE_ENTRIES):
        self.max_entries = max(1, max_entries)
        self._store: OrderedDict[str, CacheItem] = OrderedDict()
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
            self._store.move_to_end(key)
            return item

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> CacheItem:
        with self._lock:
            item = CacheItem(key, value, ttl_seconds)
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = item
            while len(self._store) > self.max_entries:
                self._store.popitem(last=False)
                CACHE_EVICTIONS.inc()
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
            now = time.monotonic()
            active_keys = [k for k, item in self._store.items() if not item.is_expired(now)]
            return {"total_keys": len(active_keys), "keys": active_keys[:50], "max_entries": self.max_entries}

    def stop(self) -> None:
        self._stop_eviction.set()

    def _eviction_loop(self) -> None:
        while not self._stop_eviction.wait(timeout=2.0):
            with self._lock:
                now = time.monotonic()
                expired = [k for k, item in self._store.items() if item.is_expired(now)]
                for k in expired:
                    del self._store[k]
                    CACHE_EVICTIONS.inc()
                if expired:
                    CACHE_KEYS_GAUGE.set(len(self._store))


cache_store = CacheStorage()
SERVER_START_TIME = time.monotonic()


class CacheRequestHandler(JsonRequestHandler):
    service_name = SERVICE_NAME

    def do_GET(self) -> None:
        start = time.monotonic()
        path = urlparse(self.path).path.rstrip("/")
        status = 200
        try:
            status = self._dispatch_get(path)
        finally:
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "GET"})

    def _dispatch_get(self, path: str) -> int:
        if path == "/health":
            stats = cache_store.stats()
            self.send_json(200, {
                "status": "healthy",
                "service": SERVICE_NAME,
                "active_keys": stats["total_keys"],
                "uptime_seconds": round(time.monotonic() - SERVER_START_TIME, 2),
            })
            return 200
        if path == "/metrics":
            self.send_metrics(metrics_reg.generate_exposition())
            return 200
        if path == "/cache":
            self.send_json(200, {"service": SERVICE_NAME, "stats": cache_store.stats()})
            return 200
        if path.startswith("/cache/"):
            key = unquote(path[len("/cache/"):]).strip()
            if not key:
                self.send_json(400, {"error": "Cache key cannot be empty", "service": SERVICE_NAME})
                return 400
            CACHE_OPERATIONS.inc(labels={"operation": "get"})
            item = cache_store.get(key)
            if item is None:
                CACHE_MISSES.inc()
                self.send_json(404, {
                    "status": "miss",
                    "error": f"Key '{key}' not found or expired",
                    "key": key,
                    "service": SERVICE_NAME,
                })
                return 404
            CACHE_HITS.inc()
            self.send_json(200, {
                "status": "hit",
                "key": item.key,
                "value": item.value,
                "ttl_remaining_seconds": item.ttl_remaining(),
                "service": SERVICE_NAME,
            })
            return 200
        self.send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})
        return 404

    def do_POST(self) -> None:
        self._handle_write("POST")

    def do_PUT(self) -> None:
        self._handle_write("PUT")

    def _handle_write(self, method: str) -> None:
        start = time.monotonic()
        path = urlparse(self.path).path.rstrip("/")
        status = 200
        try:
            payload, err = self.read_json_body(max_bytes=1_000_000)
            if err is not None:
                status, body = err
                self.send_json(status, body)
                return
            assert payload is not None
            key = payload.get("key") if path == "/cache" else (
                unquote(path[len("/cache/"):]) if path.startswith("/cache/") else None
            )
            if not key or not isinstance(key, str) or not key.strip():
                status = 400
                self.send_json(400, {"error": "Valid 'key' string is required", "service": SERVICE_NAME})
                return
            if "value" not in payload:
                status = 400
                self.send_json(400, {"error": "Field 'value' is required", "service": SERVICE_NAME})
                return
            raw_ttl = payload.get("ttl_seconds", DEFAULT_TTL)
            ttl: Optional[float] = None
            if raw_ttl is not None:
                try:
                    ttl = float(raw_ttl)
                except (TypeError, ValueError):
                    status = 400
                    self.send_json(400, {"error": "Field 'ttl_seconds' must be numeric", "service": SERVICE_NAME})
                    return
                if ttl < 0:
                    status = 400
                    self.send_json(400, {"error": "Field 'ttl_seconds' cannot be negative", "service": SERVICE_NAME})
                    return
            CACHE_OPERATIONS.inc(labels={"operation": "set"})
            item = cache_store.set(key.strip(), payload["value"], ttl)
            status = 201 if method == "POST" else 200
            self.send_json(status, {
                "status": "stored",
                "key": item.key,
                "ttl_seconds": ttl,
                "service": SERVICE_NAME,
            })
        finally:
            HTTP_REQUESTS.inc(labels={"method": method, "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": method})

    def do_DELETE(self) -> None:
        start = time.monotonic()
        path = urlparse(self.path).path.rstrip("/")
        status = 200
        try:
            if path == "/cache":
                cache_store.clear()
                self.send_json(200, {"status": "cleared", "service": SERVICE_NAME})
                return
            if path.startswith("/cache/"):
                key = unquote(path[len("/cache/"):]).strip()
                CACHE_OPERATIONS.inc(labels={"operation": "delete"})
                if not cache_store.delete(key):
                    status = 404
                    self.send_json(404, {"error": f"Key '{key}' not found", "service": SERVICE_NAME})
                    return
                self.send_json(200, {"status": "deleted", "key": key, "service": SERVICE_NAME})
                return
            status = 404
            self.send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})
        finally:
            HTTP_REQUESTS.inc(labels={"method": "DELETE", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "DELETE"})


def run_server(port: int = PORT, host: str = HOST) -> BoundedThreadingHTTPServer:
    server = BoundedThreadingHTTPServer((host, port), CacheRequestHandler)
    logger.info("data-cache-store listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    srv = run_server()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down data-cache-store")
        cache_store.stop()
        srv.shutdown()
