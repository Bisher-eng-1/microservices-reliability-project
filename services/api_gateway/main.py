"""
Service A: api-gateway
Public edge ingress gateway with finite timeouts, strict request validation,
circuit breakers, structured error envelopes, and Prometheus observability.
"""
import http.server
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

# Add workspace root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.metrics import MetricsRegistry

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"api-gateway","message":"%(message)s"}',
)
logger = logging.getLogger("api-gateway")

SERVICE_NAME = "api-gateway"
PORT = get_env_int("PORT", 8000)
HOST = get_env_str("HOST", "0.0.0.0")

WORKER_SERVICE_URL = get_env_str("WORKER_SERVICE_URL", "http://backend-worker:8001")
CACHE_SERVICE_URL = get_env_str("CACHE_SERVICE_URL", "http://data-cache-store:8002")
DOWNSTREAM_TIMEOUT = get_env_float("DOWNSTREAM_TIMEOUT_SECONDS", 3.0)

# Metrics Registry
metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter(
    "http_requests_total", "Total incoming HTTP requests received by api-gateway"
)
HTTP_DURATION = metrics_reg.register_histogram(
    "http_request_duration_seconds", "HTTP request processing latency in seconds for api-gateway"
)
DOWNSTREAM_REQUESTS = metrics_reg.register_counter(
    "downstream_requests_total", "Requests forwarded from gateway to internal microservices"
)
DOWNSTREAM_FAILURES = metrics_reg.register_counter(
    "downstream_failures_total", "Failures encountered when calling downstream microservices"
)
VALIDATION_ERRORS = metrics_reg.register_counter(
    "request_validation_errors_total", "Malformed or invalid requests rejected at gateway boundary"
)

# Resilient Downstream Clients
worker_client = ResilientHttpClient("backend-worker", default_timeout_seconds=DOWNSTREAM_TIMEOUT)
cache_client = ResilientHttpClient("data-cache-store", default_timeout_seconds=DOWNSTREAM_TIMEOUT)


class GatewayRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_json(self, status: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-Id")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-Id")
        self.end_headers()

    def do_GET(self) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            # Health Check
            if path == "/health":
                self._send_json(200, {
                    "status": "healthy",
                    "service": SERVICE_NAME,
                    "downstream_circuits": {
                        "backend-worker": worker_client.circuit_breaker.get_status(),
                        "data-cache-store": cache_client.circuit_breaker.get_status(),
                    },
                    "uptime_seconds": round(time.time() - SERVER_START_TIME, 2),
                })
                return

            # Prometheus Metrics
            if path == "/metrics":
                exposition = metrics_reg.generate_exposition().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Content-Length", str(len(exposition)))
                self.end_headers()
                self.wfile.write(exposition)
                return

            # Aggregated System Status
            if path == "/api/status":
                w_status, w_data = worker_client.request("GET", f"{WORKER_SERVICE_URL}/health", timeout=1.5)
                c_status, c_data = cache_client.request("GET", f"{CACHE_SERVICE_URL}/health", timeout=1.5)

                DOWNSTREAM_REQUESTS.inc(amount=2.0)
                if w_status != 200:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "backend-worker"})
                if c_status != 200:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "data-cache-store"})

                system_healthy = (w_status == 200 and c_status == 200)
                status = 200 if system_healthy else 207  # Multi-Status partial degradation
                self._send_json(status, {
                    "gateway": "healthy",
                    "dependencies": {
                        "backend_worker": {"status_code": w_status, "data": w_data},
                        "data_cache_store": {"status_code": c_status, "data": c_data},
                    },
                    "system_healthy": system_healthy,
                })
                return

            # Route: GET /api/jobs -> List recent jobs from backend-worker
            if path == "/api/jobs":
                DOWNSTREAM_REQUESTS.inc(labels={"target": "backend-worker"})
                w_status, w_data = worker_client.request("GET", f"{WORKER_SERVICE_URL}/jobs")
                if w_status >= 500:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "backend-worker"})
                status = w_status
                self._send_json(status, w_data)
                return

            # Route: GET /api/jobs/<job_id> -> Query job from backend-worker
            if path.startswith("/api/jobs/"):
                job_id = path[len("/api/jobs/"):]
                if not job_id:
                    status = 400
                    VALIDATION_ERRORS.inc()
                    self._send_json(400, {"error": "Missing job_id in path", "service": SERVICE_NAME})
                    return

                DOWNSTREAM_REQUESTS.inc(labels={"target": "backend-worker"})
                w_status, w_data = worker_client.request("GET", f"{WORKER_SERVICE_URL}/jobs/{job_id}")
                if w_status >= 500:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "backend-worker"})
                status = w_status
                self._send_json(status, w_data)
                return

            # Route: GET /api/cache/<key> -> Read from data-cache-store
            if path.startswith("/api/cache/"):
                key = path[len("/api/cache/"):]
                if not key:
                    status = 400
                    VALIDATION_ERRORS.inc()
                    self._send_json(400, {"error": "Missing cache key in path", "service": SERVICE_NAME})
                    return

                DOWNSTREAM_REQUESTS.inc(labels={"target": "data-cache-store"})
                c_status, c_data = cache_client.request("GET", f"{CACHE_SERVICE_URL}/cache/{key}")
                if c_status >= 500:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "data-cache-store"})
                status = c_status
                self._send_json(status, c_data)
                return

            status = 404
            self._send_json(404, {"error": f"Route '{path}' not found on api-gateway", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "GET"})

    def do_POST(self) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > 256_000:
                status = 413
                VALIDATION_ERRORS.inc()
                self._send_json(413, {"error": "Payload size exceeds 256KB threshold", "service": SERVICE_NAME})
                return

            raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
            try:
                body_json = json.loads(raw_body)
            except json.JSONDecodeError as err:
                status = 400
                VALIDATION_ERRORS.inc()
                self._send_json(400, {
                    "error": "Malformed JSON payload in request body",
                    "details": str(err),
                    "service": SERVICE_NAME,
                })
                return

            # Route: POST /api/jobs -> Submit job to backend-worker
            if path == "/api/jobs":
                if not isinstance(body_json, dict) or "payload" not in body_json:
                    status = 400
                    VALIDATION_ERRORS.inc()
                    self._send_json(400, {
                        "error": "Validation failed: 'payload' property is required in request body",
                        "service": SERVICE_NAME,
                    })
                    return

                DOWNSTREAM_REQUESTS.inc(labels={"target": "backend-worker"})
                w_status, w_data = worker_client.request("POST", f"{WORKER_SERVICE_URL}/jobs", payload=body_json)
                if w_status >= 500:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "backend-worker"})

                status = w_status
                self._send_json(status, w_data)
                return

            # Route: POST /api/cache -> Manual write to cache
            if path == "/api/cache":
                DOWNSTREAM_REQUESTS.inc(labels={"target": "data-cache-store"})
                c_status, c_data = cache_client.request("POST", f"{CACHE_SERVICE_URL}/cache", payload=body_json)
                if c_status >= 500:
                    DOWNSTREAM_FAILURES.inc(labels={"target": "data-cache-store"})
                status = c_status
                self._send_json(status, c_data)
                return

            status = 404
            self._send_json(404, {"error": f"Route '{path}' not found on api-gateway", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "POST"})


SERVER_START_TIME = time.time()


def run_server(port: int = PORT, host: str = HOST) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer((host, port), GatewayRequestHandler)
    logger.info("Service A [api-gateway] listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    server = run_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down api-gateway...")
        server.shutdown()
