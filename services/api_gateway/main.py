import time
from typing import Any, Dict, Tuple
from urllib.parse import unquote, urlparse
import uuid

from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.http_server import (
    BoundedThreadingHTTPServer,
    JsonRequestHandler,
    configure_service_logger,
)
from services.common.metrics import MetricsRegistry

SERVICE_NAME = "api-gateway"
logger = configure_service_logger(SERVICE_NAME)

PORT = get_env_int("PORT", 8000)
HOST = get_env_str("HOST", "0.0.0.0")
WORKER_SERVICE_URL = get_env_str("WORKER_SERVICE_URL", "http://backend-worker:8001")
CACHE_SERVICE_URL = get_env_str("CACHE_SERVICE_URL", "http://data-cache-store:8002")
DOWNSTREAM_TIMEOUT = get_env_float("DOWNSTREAM_TIMEOUT_SECONDS", 3.0)

metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter("http_requests_total", "Total HTTP requests processed")
HTTP_DURATION = metrics_reg.register_histogram("http_request_duration_seconds", "HTTP request latency in seconds")
DOWNSTREAM_REQUESTS = metrics_reg.register_counter("downstream_requests_total", "Downstream HTTP calls")
DOWNSTREAM_FAILURES = metrics_reg.register_counter("downstream_failures_total", "Downstream call failures")
VALIDATION_ERRORS = metrics_reg.register_counter("request_validation_errors_total", "Rejected invalid requests")

worker_client = ResilientHttpClient("backend-worker", default_timeout_seconds=DOWNSTREAM_TIMEOUT)
cache_client = ResilientHttpClient("data-cache-store", default_timeout_seconds=DOWNSTREAM_TIMEOUT)
SERVER_START_TIME = time.monotonic()


class GatewayRequestHandler(JsonRequestHandler):
    service_name = SERVICE_NAME

    def _trace_headers(self) -> Dict[str, str]:
        req_id = self.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]
        return {"X-Request-Id": req_id}

    def _call_downstream(
        self,
        target: str,
        client: ResilientHttpClient,
        method: str,
        url: str,
        payload: Any = None,
        timeout: Any = None,
    ) -> Tuple[int, Dict[str, Any]]:
        DOWNSTREAM_REQUESTS.inc(labels={"target": target})
        status, data = client.request(method, url, payload=payload, headers=self._trace_headers(), timeout=timeout)
        if status >= 500:
            DOWNSTREAM_FAILURES.inc(labels={"target": target})
        return status, data

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
            self.send_json(200, {
                "status": "healthy",
                "service": SERVICE_NAME,
                "downstream_circuits": {
                    "backend-worker": worker_client.circuit_breaker.get_status(),
                    "data-cache-store": cache_client.circuit_breaker.get_status(),
                },
                "uptime_seconds": round(time.monotonic() - SERVER_START_TIME, 2),
            })
            return 200
        if path == "/metrics":
            self.send_metrics(metrics_reg.generate_exposition())
            return 200
        if path == "/api/status":
            w_status, w_data = self._call_downstream(
                "backend-worker", worker_client, "GET", f"{WORKER_SERVICE_URL}/health", timeout=1.5
            )
            c_status, c_data = self._call_downstream(
                "data-cache-store", cache_client, "GET", f"{CACHE_SERVICE_URL}/health", timeout=1.5
            )
            system_healthy = w_status == 200 and c_status == 200
            status = 200 if system_healthy else 207
            self.send_json(status, {
                "gateway": "healthy",
                "dependencies": {
                    "backend_worker": {"status_code": w_status, "data": w_data},
                    "data_cache_store": {"status_code": c_status, "data": c_data},
                },
                "system_healthy": system_healthy,
            })
            return status
        if path == "/api/jobs":
            w_status, w_data = self._call_downstream(
                "backend-worker", worker_client, "GET", f"{WORKER_SERVICE_URL}/jobs"
            )
            self.send_json(w_status, w_data)
            return w_status
        if path.startswith("/api/jobs/"):
            job_id = unquote(path[len("/api/jobs/"):]).strip()
            if not job_id:
                VALIDATION_ERRORS.inc()
                self.send_json(400, {"error": "Missing job_id in path", "service": SERVICE_NAME})
                return 400
            w_status, w_data = self._call_downstream(
                "backend-worker", worker_client, "GET", f"{WORKER_SERVICE_URL}/jobs/{job_id}"
            )
            self.send_json(w_status, w_data)
            return w_status
        if path.startswith("/api/cache/"):
            key = unquote(path[len("/api/cache/"):]).strip()
            if not key:
                VALIDATION_ERRORS.inc()
                self.send_json(400, {"error": "Missing cache key in path", "service": SERVICE_NAME})
                return 400
            c_status, c_data = self._call_downstream(
                "data-cache-store", cache_client, "GET", f"{CACHE_SERVICE_URL}/cache/{key}"
            )
            self.send_json(c_status, c_data)
            return c_status
        self.send_json(404, {"error": f"Route '{path}' not found on api-gateway", "service": SERVICE_NAME})
        return 404

    def do_POST(self) -> None:
        start = time.monotonic()
        path = urlparse(self.path).path.rstrip("/")
        status = 200
        try:
            status = self._dispatch_post(path)
        finally:
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "POST"})

    def _dispatch_post(self, path: str) -> int:
        body_json, err = self.read_json_body(max_bytes=256_000)
        if err is not None:
            status, body = err
            VALIDATION_ERRORS.inc()
            self.send_json(status, body)
            return status
        assert body_json is not None

        if path == "/api/jobs":
            if "payload" not in body_json:
                VALIDATION_ERRORS.inc()
                self.send_json(400, {
                    "error": "Validation failed: 'payload' property is required in request body",
                    "service": SERVICE_NAME,
                })
                return 400
            w_status, w_data = self._call_downstream(
                "backend-worker", worker_client, "POST", f"{WORKER_SERVICE_URL}/jobs", payload=body_json
            )
            self.send_json(w_status, w_data)
            return w_status
        if path == "/api/cache":
            c_status, c_data = self._call_downstream(
                "data-cache-store", cache_client, "POST", f"{CACHE_SERVICE_URL}/cache", payload=body_json
            )
            self.send_json(c_status, c_data)
            return c_status
        self.send_json(404, {"error": f"Route '{path}' not found on api-gateway", "service": SERVICE_NAME})
        return 404


def run_server(port: int = PORT, host: str = HOST) -> BoundedThreadingHTTPServer:
    server = BoundedThreadingHTTPServer((host, port), GatewayRequestHandler)
    logger.info("api-gateway listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    srv = run_server()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down api-gateway")
        srv.shutdown()
