from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import math
import threading
import time
from typing import Any, Dict, List, Optional
import uuid

from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.http_server import (
    BoundedThreadingHTTPServer,
    JsonRequestHandler,
    configure_service_logger,
)
from services.common.metrics import MetricsRegistry

SERVICE_NAME = "backend-worker"
logger = configure_service_logger(SERVICE_NAME)

PORT = get_env_int("PORT", 8001)
HOST = get_env_str("HOST", "0.0.0.0")
CACHE_STORE_URL = get_env_str("CACHE_STORE_URL", "http://data-cache-store:8002")
CACHE_TIMEOUT = get_env_float("CACHE_TIMEOUT_SECONDS", 2.0)
WORKER_CONCURRENCY = get_env_int("WORKER_CONCURRENCY", 4)
JOB_CACHE_TTL = get_env_int("JOB_CACHE_TTL_SECONDS", 180)
MAX_RETAINED_JOBS = get_env_int("MAX_RETAINED_JOBS", 1000)

metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter("http_requests_total", "Total HTTP requests processed")
HTTP_DURATION = metrics_reg.register_histogram("http_request_duration_seconds", "HTTP request latency in seconds")
JOBS_PROCESSED = metrics_reg.register_counter("jobs_processed_total", "Total jobs completed")
JOBS_FAILED = metrics_reg.register_counter("jobs_failed_total", "Total jobs failed")
DEPENDENCY_FAILURES = metrics_reg.register_counter(
    "dependency_request_failures_total", "Downstream dependency failures"
)
QUEUE_DEPTH = metrics_reg.register_gauge("worker_queue_depth", "Queued jobs awaiting execution")
ACTIVE_WORKERS = metrics_reg.register_gauge("worker_active_threads", "Worker threads executing jobs")
JOB_DURATION = metrics_reg.register_histogram("job_processing_duration_seconds", "Job execution duration in seconds")

cache_client = ResilientHttpClient("data-cache-store", default_timeout_seconds=CACHE_TIMEOUT)


class JobStatus:
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Job:
    def __init__(
        self,
        job_id: str,
        payload: str,
        job_type: str = "text_analysis",
        simulate_delay: float = 0.0,
        force_fail: bool = False,
    ):
        self.job_id = job_id
        self.payload = payload
        self.job_type = job_type
        self.simulate_delay = simulate_delay
        self.force_fail = force_fail
        self.status = JobStatus.PENDING
        self.created_at = time.time()
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.duration_seconds: Optional[float] = None
        self.result: Optional[Dict[str, Any]] = None
        self.error: Optional[str] = None
        self.cache_hit = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "job_type": self.job_type,
            "payload_preview": self.payload[:60] if self.payload else "",
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "cache_hit": self.cache_hit,
            "result": self.result,
            "error": self.error,
        }


def _analyze_payload(payload: str, payload_hash: str) -> Dict[str, Any]:
    char_count = len(payload)
    word_count = len(payload.split())
    vowels = sum(1 for c in payload.lower() if c in "aeiou")
    entropy = 0.0
    if char_count > 0:
        freq: Dict[str, int] = {}
        for ch in payload:
            freq[ch] = freq.get(ch, 0) + 1
        for count in freq.values():
            p = count / char_count
            entropy -= p * math.log2(p)
    return {
        "sha256": payload_hash,
        "character_count": char_count,
        "word_count": word_count,
        "vowel_count": vowels,
        "shannon_entropy": round(entropy, 4),
        "processed_by": SERVICE_NAME,
        "completed_timestamp": time.time(),
    }


class JobManager:
    def __init__(self, max_workers: int = WORKER_CONCURRENCY, max_retained_jobs: int = MAX_RETAINED_JOBS):
        self.max_retained_jobs = max(1, max_retained_jobs)
        self.jobs: OrderedDict[str, Job] = OrderedDict()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker-pool")

    def _evict_if_full(self) -> None:
        if len(self.jobs) < self.max_retained_jobs:
            return
        for key, existing in list(self.jobs.items()):
            if existing.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                del self.jobs[key]
                if len(self.jobs) < self.max_retained_jobs:
                    return
        while len(self.jobs) >= self.max_retained_jobs:
            self.jobs.popitem(last=False)

    def submit_job(
        self,
        payload: str,
        job_type: str = "text_analysis",
        simulate_delay: float = 0.0,
        force_fail: bool = False,
    ) -> Job:
        job_id = f"job-{uuid.uuid4().hex[:12]}"
        job = Job(job_id, payload, job_type, simulate_delay, force_fail)
        with self.lock:
            self._evict_if_full()
            self.jobs[job_id] = job
            QUEUE_DEPTH.inc()
        self.executor.submit(self._execute_job, job)
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        with self.lock:
            return self.jobs.get(job_id)

    def list_jobs(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self.lock:
            recent = list(reversed(self.jobs.values()))[:limit]
            return [j.to_dict() for j in recent]

    def _lookup_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        try:
            c_status, c_resp = cache_client.request("GET", f"{CACHE_STORE_URL}/cache/{cache_key}")
            if c_status == 200 and c_resp.get("status") == "hit":
                return c_resp.get("value")
            if c_status >= 500:
                DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
        except Exception as exc:
            DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
            logger.warning("Cache lookup error: %s", exc)
        return None

    def _write_cache(self, cache_key: str, result_data: Dict[str, Any]) -> None:
        try:
            c_status, _ = cache_client.request(
                "POST",
                f"{CACHE_STORE_URL}/cache",
                payload={"key": cache_key, "value": result_data, "ttl_seconds": JOB_CACHE_TTL},
            )
            if c_status >= 500:
                DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
        except Exception as exc:
            DEPENDENCY_FAILURES.inc(labels={"dependency": "data-cache-store"})
            logger.warning("Cache write error: %s", exc)

    def _execute_job(self, job: Job) -> None:
        QUEUE_DEPTH.dec()
        ACTIVE_WORKERS.inc()
        job.status = JobStatus.PROCESSING
        job.started_at = time.time()
        start_mono = time.monotonic()
        try:
            if job.force_fail:
                time.sleep(0.1)
                raise RuntimeError("Simulated worker processing failure induced by request parameter 'force_fail=true'")

            payload_hash = hashlib.sha256(job.payload.encode("utf-8")).hexdigest()
            cache_key = f"job_result_{payload_hash}"
            cached = self._lookup_cache(cache_key)
            if cached is not None:
                job.cache_hit = True
                job.result = cached
                job.status = JobStatus.COMPLETED
                job.completed_at = time.time()
                elapsed = time.monotonic() - start_mono
                job.duration_seconds = round(elapsed, 4)
                JOBS_PROCESSED.inc(labels={"type": job.job_type, "cache_hit": "true"})
                JOB_DURATION.observe(elapsed)
                return

            if job.simulate_delay > 0:
                time.sleep(job.simulate_delay)

            result_data = _analyze_payload(job.payload, payload_hash)
            self._write_cache(cache_key, result_data)

            job.result = result_data
            job.status = JobStatus.COMPLETED
            job.completed_at = time.time()
            elapsed = time.monotonic() - start_mono
            job.duration_seconds = round(elapsed, 4)
            JOBS_PROCESSED.inc(labels={"type": job.job_type, "cache_hit": "false"})
            JOB_DURATION.observe(elapsed)
        except Exception as ex:
            job.status = JobStatus.FAILED
            job.error = str(ex)
            job.completed_at = time.time()
            job.duration_seconds = round(time.monotonic() - start_mono, 4)
            JOBS_FAILED.inc(labels={"type": job.job_type})
            logger.error("Job [%s] failed: %s", job.job_id, ex)
        finally:
            ACTIVE_WORKERS.dec()


job_manager = JobManager()
SERVER_START_TIME = time.monotonic()


class WorkerRequestHandler(JsonRequestHandler):
    service_name = SERVICE_NAME

    def do_GET(self) -> None:
        start = time.monotonic()
        path = self.path.split("?")[0].rstrip("/")
        status = 200
        try:
            status = self._dispatch_get(path)
        finally:
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "GET"})

    def _dispatch_get(self, path: str) -> int:
        if path == "/health":
            c_status, _ = cache_client.request("GET", f"{CACHE_STORE_URL}/health", timeout=1.0)
            self.send_json(200, {
                "status": "healthy",
                "service": SERVICE_NAME,
                "dependencies": {
                    "data-cache-store": "connected" if c_status == 200 else "degraded",
                    "cache_status_code": c_status,
                },
                "circuit_breaker": cache_client.circuit_breaker.get_status(),
                "uptime_seconds": round(time.monotonic() - SERVER_START_TIME, 2),
            })
            return 200
        if path == "/metrics":
            self.send_metrics(metrics_reg.generate_exposition())
            return 200
        if path == "/jobs":
            jobs_list = job_manager.list_jobs(50)
            self.send_json(200, {"jobs": jobs_list, "total": len(jobs_list), "service": SERVICE_NAME})
            return 200
        if path.startswith("/jobs/"):
            job_id = path[len("/jobs/"):]
            job = job_manager.get_job(job_id)
            if job is None:
                self.send_json(404, {"error": f"Job '{job_id}' not found", "service": SERVICE_NAME})
                return 404
            self.send_json(200, job.to_dict())
            return 200
        self.send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})
        return 404

    def do_POST(self) -> None:
        start = time.monotonic()
        path = self.path.split("?")[0].rstrip("/")
        status = 202
        try:
            if path != "/jobs":
                status = 404
                self.send_json(404, {"error": f"Endpoint '{path}' not found", "service": SERVICE_NAME})
                return
            payload_json, err = self.read_json_body(max_bytes=500_000)
            if err is not None:
                status, body = err
                self.send_json(status, body)
                return
            assert payload_json is not None
            if "payload" not in payload_json:
                status = 400
                self.send_json(400, {"error": "Required field 'payload' is missing", "service": SERVICE_NAME})
                return
            try:
                simulate_delay = float(payload_json.get("simulate_delay", 0.0))
            except (TypeError, ValueError):
                status = 400
                self.send_json(400, {"error": "Field 'simulate_delay' must be numeric", "service": SERVICE_NAME})
                return
            if simulate_delay < 0 or simulate_delay > 10.0:
                status = 400
                self.send_json(
                    400, {"error": "Field 'simulate_delay' must be between 0 and 10s", "service": SERVICE_NAME}
                )
                return

            job = job_manager.submit_job(
                payload=str(payload_json.get("payload", "")),
                job_type=str(payload_json.get("job_type", "text_analysis")),
                simulate_delay=simulate_delay,
                force_fail=bool(payload_json.get("force_fail", False)),
            )
            self.send_json(202, {
                "job_id": job.job_id,
                "status": job.status,
                "message": "Job accepted for processing",
                "poll_url": f"/jobs/{job.job_id}",
                "service": SERVICE_NAME,
            })
        finally:
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "POST"})


def run_server(port: int = PORT, host: str = HOST) -> BoundedThreadingHTTPServer:
    server = BoundedThreadingHTTPServer((host, port), WorkerRequestHandler)
    logger.info("backend-worker listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    srv = run_server()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down backend-worker")
        srv.shutdown()
