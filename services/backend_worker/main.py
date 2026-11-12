"""
Service C: backend-worker
Asynchronous job processor decoupled from frontend.
Features:
- Threaded job queue with worker pool.
- Job lifecycle tracking (PENDING -> PROCESSING -> COMPLETED / FAILED).
- Cryptographic hash + lexical analysis compute pipeline.
- Resilient downstream caching to data-cache-store.
- Graceful degradation when cache dependency is unavailable.
- Prometheus observability and health checks.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.server
import json
import logging
import math
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional
import uuid

# Add workspace root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.metrics import MetricsRegistry

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"backend-worker","message":"%(message)s"}',
)
logger = logging.getLogger("backend-worker")

SERVICE_NAME = "backend-worker"
PORT = get_env_int("PORT", 8001)
HOST = get_env_str("HOST", "0.0.0.0")
CACHE_STORE_URL = get_env_str("CACHE_STORE_URL", "http://data-cache-store:8002")
CACHE_TIMEOUT = get_env_float("CACHE_TIMEOUT_SECONDS", 2.0)
WORKER_CONCURRENCY = get_env_int("WORKER_CONCURRENCY", 4)
JOB_CACHE_TTL = get_env_int("JOB_CACHE_TTL_SECONDS", 180)

# Observability Metrics Registry
metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter(
    "http_requests_total", "Total count of HTTP requests processed by backend-worker"
)
HTTP_DURATION = metrics_reg.register_histogram(
    "http_request_duration_seconds", "HTTP request latency in seconds for backend-worker"
)
JOBS_PROCESSED = metrics_reg.register_counter(
    "jobs_processed_total", "Total jobs successfully processed by backend-worker"
)
JOBS_FAILED = metrics_reg.register_counter(
    "jobs_failed_total", "Total jobs that failed execution"
)
DEPENDENCY_FAILURES = metrics_reg.register_counter(
    "dependency_request_failures_total", "Total downstream failures contacting dependencies (e.g. data-cache-store)"
)
QUEUE_DEPTH = metrics_reg.register_gauge(
    "worker_queue_depth", "Current number of jobs queued in memory waiting for execution"
)
ACTIVE_WORKERS = metrics_reg.register_gauge(
    "worker_active_threads", "Number of worker threads currently processing jobs"
)
JOB_DURATION = metrics_reg.register_histogram(
    "job_processing_duration_seconds", "Job execution duration in seconds"
)

# Resilient HTTP Client for Cache Service
cache_client = ResilientHttpClient("data-cache-store", default_timeout_seconds=CACHE_TIMEOUT)


class JobStatus:
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Job:
    def __init__(self, job_id: str, payload: str, job_type: str = "text_analysis",
                 simulate_delay: float = 0.0, force_fail: bool = False):
        self.job_id = job_id
        self.payload = payload
        self.job_type = job_type
        self.simulate_delay = simulate_delay
        self.force_fail = force_fail
        self.status = JobStatus.PENDING
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.cache_hit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "job_type": self.job_type,
            "payload_preview": self.payload[:60] if self.payload else "",
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": round(self.completed_at - self.started_at, 4)
            if (self.started_at and self.completed_at)
            else None,
            "cache_hit": self.cache_hit,
            "result": self.result,
            "error": self.error,
        }


class JobManager:
    """
    Manages in-memory job queue and thread pool.
    NOTE FOR DISTRIBUTED SYSTEMS INTERVIEWS:
    This queue is stored in process memory. It is appropriate for bounded demonstration,
    reproducible local execution, and integration testing. In full-scale enterprise production,
    this would be backed by a durable message broker (e.g. RabbitMQ or Apache Kafka)
    to provide persistence across worker container restarts and distributed consumer groups.
    """
    def __init__(self, max_workers: int = WORKER_CONCURRENCY):
        self.jobs: Dict[str, Job] = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker-pool")

    def submit_job(self, payload: str, job_type: str = "text_analysis",
                   simulate_delay: float = 0.0, force_fail: bool = False) -> Job:
        job_id = f"job-{uuid.uuid4().hex[:12]}"
        job = Job(job_id, payload, job_type, simulate_delay, force_fail)
        with self.lock:
            self.jobs[job_id] = job
            QUEUE_DEPTH.inc()

        logger.info("Job [%s] queued. Type: %s, Delay: %.2fs", job_id, job_type, simulate_delay)
        self.executor.submit(self._execute_job, job)
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        with self.lock:
            return self.jobs.get(job_id)

    def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock:
            sorted_jobs = sorted(self.jobs.values(), key=lambda j: j.created_at, reverse=True)
            return [j.to_dict() for j in sorted_jobs[:limit]]

    def _execute_job(self, job: Job) -> None:
        QUEUE_DEPTH.dec()
        ACTIVE_WORKERS.inc()
        job.status = JobStatus.PROCESSING
        job.started_at = time.time()
        start_t = job.started_at

        try:
            # Check for simulated failure injection
            if job.force_fail:
                time.sleep(0.1)
                raise RuntimeError("Simulated worker processing failure induced by request parameter 'force_fail=true'")

            # Compute payload fingerprint for caching
            payload_bytes = job.payload.encode("utf-8")
            payload_hash = hashlib.sha256(payload_bytes).hexdigest()
            cache_key = f"job_result_{payload_hash}"

            # Step 1: Check downstream cache
            cache_hit_data = None
            try:
                cache_status, cache_resp = cache_client.request("GET", f"{CACHE_STORE_URL}/cache/{cache_key}")
                if cache_status == 200 and cache_resp.get("status") == "hit":
                    cache_hit_data = cache_resp.get("value")
                    logger.info("Cache hit for Job [%s] on key %s", job.job_id, cache_key)
                elif cache_status >= 500:
                    DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
                    logger.warning("Cache service error (%d) when querying [%s]", cache_status, cache_key)
            except Exception as e:
                DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
                logger.warning("Failed communicating with data-cache-store: %s", str(e))

            if cache_hit_data:
                job.cache_hit = True
                job.result = cache_hit_data
                job.status = JobStatus.COMPLETED
                job.completed_at = time.time()
                JOBS_PROCESSED.inc(labels={"type": job.job_type, "cache_hit": "true"})
                JOB_DURATION.observe(job.completed_at - start_t)
                return

            # Step 2: Simulate compute delay if requested
            if job.simulate_delay > 0:
                time.sleep(job.simulate_delay)

            # Step 3: Perform meaningful computation: Lexical analysis & Cryptographic metrics
            words = job.payload.split()
            word_count = len(words)
            char_count = len(job.payload)
            vowels = sum(1 for c in job.payload.lower() if c in "aeiou")
            entropy = 0.0
            if char_count > 0:
                freq: Dict[str, int] = {}
                for c in job.payload:
                    freq[c] = freq.get(c, 0) + 1
                for count in freq.values():
                    p = count / char_count
                    entropy -= p * math.log2(p)

            result_data = {
                "sha256": payload_hash,
                "character_count": char_count,
                "word_count": word_count,
                "vowel_count": vowels,
                "shannon_entropy": round(entropy, 4),
                "processed_by": SERVICE_NAME,
                "completed_timestamp": time.time(),
            }

            # Step 4: Write result to downstream cache
            try:
                c_status, _ = cache_client.request(
                    "POST",
                    f"{CACHE_STORE_URL}/cache",
                    payload={"key": cache_key, "value": result_data, "ttl_seconds": JOB_CACHE_TTL},
                )
                if c_status >= 500:
                    DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
                    logger.warning("Cache write returned status %d for key [%s]", c_status, cache_key)
            except Exception as e:
                DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
                logger.warning("Failed writing result to cache store: %s", str(e))

            job.result = result_data
            job.status = JobStatus.COMPLETED
            job.completed_at = time.time()
            JOBS_PROCESSED.inc(labels={"type": job.job_type, "cache_hit": "false"})
            JOB_DURATION.observe(job.completed_at - start_t)
            logger.info("Job [%s] completed successfully in %.4fs", job.job_id, job.completed_at - start_t)

        except Exception as ex:
            job.status = JobStatus.FAILED
            job.error = str(ex)
            job.completed_at = time.time()
            JOBS_FAILED.inc(labels={"type": job.job_type})
            logger.error("Job [%s] failed: %s", job.job_id, str(ex))

        finally:
            ACTIVE_WORKERS.dec()


job_manager = JobManager()


class WorkerRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_json(self, status: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        start_time = time.time()
        path = self.path.split("?")[0].rstrip("/")
        status = 200

        try:
            if path == "/health":
                # Quick non-blocking probe to cache
                c_status, c_data = cache_client.request("GET", f"{CACHE_STORE_URL}/health", timeout=1.0)
                cache_healthy = (c_status == 200)
                self._send_json(200, {
                    "status": "healthy",
                    "service": SERVICE_NAME,
                    "dependencies": {
                        "data-cache-store": "connected" if cache_healthy else "degraded",
                        "cache_status_code": c_status,
                    },
                    "circuit_breaker": cache_client.circuit_breaker.get_status(),
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

            if path == "/jobs":
                jobs_list = job_manager.list_jobs(50)
                self._send_json(200, {"jobs": jobs_list, "total": len(jobs_list), "service": SERVICE_NAME})
                return

            if path.startswith("/jobs/"):
                job_id = path[len("/jobs/"):]
                job = job_manager.get_job(job_id)
                if job is None:
                    status = 404
                    self._send_json(404, {"error": f"Job '{job_id}' not found", "service": SERVICE_NAME})
                    return
                self._send_json(200, job.to_dict())
                return

            status = 404
            self._send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "GET"})

    def do_POST(self) -> None:
        start_time = time.time()
        path = self.path.split("?")[0].rstrip("/")
        status = 202

        try:
            if path == "/jobs":
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length > 500_000:
                    status = 413
                    self._send_json(413, {"error": "Payload exceeds 500KB limit", "service": SERVICE_NAME})
                    return

                raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
                try:
                    payload_json = json.loads(raw_body)
                except json.JSONDecodeError as err:
                    status = 400
                    self._send_json(400, {"error": "Invalid JSON payload", "details": str(err), "service": SERVICE_NAME})
                    return

                if not isinstance(payload_json, dict) or "payload" not in payload_json:
                    status = 400
                    self._send_json(400, {"error": "Required field 'payload' is missing", "service": SERVICE_NAME})
                    return

                raw_payload = str(payload_json.get("payload", ""))
                job_type = str(payload_json.get("job_type", "text_analysis"))
                simulate_delay = float(payload_json.get("simulate_delay", 0.0))
                force_fail = bool(payload_json.get("force_fail", False))

                job = job_manager.submit_job(
                    payload=raw_payload,
                    job_type=job_type,
                    simulate_delay=simulate_delay,
                    force_fail=force_fail,
                )

                self._send_json(202, {
                    "job_id": job.job_id,
                    "status": job.status,
                    "message": "Job accepted for processing",
                    "poll_url": f"/jobs/{job.job_id}",
                    "service": SERVICE_NAME,
                })
                return

            status = 404
            self._send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "POST"})


SERVER_START_TIME = time.time()


def run_server(port: int = PORT, host: str = HOST) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer((host, port), WorkerRequestHandler)
    logger.info("Service C [backend-worker] listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    server = run_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down backend-worker...")
        server.shutdown()
