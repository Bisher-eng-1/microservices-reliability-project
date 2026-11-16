from concurrent.futures import ThreadPoolExecutor
import time
import unittest
import urllib.error
import urllib.request

from services.common.http_client import CircuitBreaker, ResilientHttpClient
from services.common.http_server import BoundedThreadingHTTPServer, JsonRequestHandler
from services.data_cache_store.main import CacheStorage
from tests.harness import http_request, start_test_cluster, stop_test_cluster


class _SlowHandler(JsonRequestHandler):
    def do_GET(self) -> None:
        time.sleep(0.25)
        self.send_json(200, {"ok": True})


class TestMicroservicesReliability(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls) -> None:
        stop_test_cluster()

    def test_01_invalid_payload_rejection(self) -> None:
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload={"invalid_field": 123})
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertEqual(data.get("service"), "api-gateway")

    def test_02_unknown_job_id_returns_404(self) -> None:
        status, data = http_request("GET", f"{self.urls['gateway']}/api/jobs/job-nonexistent-99999")
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_03_forced_worker_failure_handling(self) -> None:
        payload = {"payload": "Crash test", "simulate_delay": 0.05, "force_fail": True}
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        self.assertEqual(status, 202)
        job_id = data["job_id"]

        time.sleep(0.25)
        status, job = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("Simulated worker processing failure", job["error"])

    def test_04_circuit_breaker_transitions(self) -> None:
        cb = CircuitBreaker("test-service", failure_threshold=3, cooldown_seconds=0.3)
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        self.assertTrue(cb.allow_request())

        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_OPEN)
        self.assertFalse(cb.allow_request())

        time.sleep(0.35)
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitBreaker.STATE_HALF_OPEN)
        cb.record_success()
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        self.assertEqual(cb.failure_count, 0)

    def test_05_half_open_allows_single_probe_only(self) -> None:
        cb = CircuitBreaker("probe-test", failure_threshold=2, cooldown_seconds=0.15)
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_OPEN)

        time.sleep(0.2)
        with ThreadPoolExecutor(max_workers=6) as pool:
            decisions = list(pool.map(lambda _: cb.allow_request(), range(6)))

        self.assertEqual(sum(decisions), 1)
        self.assertEqual(cb.state, CircuitBreaker.STATE_HALF_OPEN)

        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_OPEN)
        self.assertFalse(cb.allow_request())

    def test_06_unreachable_downstream_handling(self) -> None:
        client = ResilientHttpClient("ghost-service", default_timeout_seconds=0.5)
        status, data = client.request("GET", "http://127.0.0.1:54321/ghost")
        self.assertEqual(status, 503)
        self.assertTrue(data.get("transient"))

    def test_07_concurrent_job_submissions(self) -> None:
        payloads = [{"payload": f"Concurrent task {i}", "simulate_delay": 0.05} for i in range(10)]
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(
                executor.map(lambda p: http_request("POST", f"{self.urls['gateway']}/api/jobs", p), payloads)
            )
        for status, data in results:
            self.assertEqual(status, 202)
            self.assertIn("job_id", data)

    def test_08_invalid_worker_delay_and_negative_ttl_rejected(self) -> None:
        status, _ = http_request(
            "POST",
            f"{self.urls['gateway']}/api/jobs",
            payload={"payload": "bad delay", "simulate_delay": -1.0},
        )
        self.assertEqual(status, 400)

        status, _ = http_request(
            "POST",
            f"{self.urls['cache']}/cache",
            payload={"key": "neg_ttl", "value": 1, "ttl_seconds": -5},
        )
        self.assertEqual(status, 400)

    def test_09_cache_lru_capacity_eviction(self) -> None:
        store = CacheStorage(max_entries=3)
        try:
            store.set("k1", 1)
            store.set("k2", 2)
            store.set("k3", 3)
            self.assertIsNotNone(store.get("k1"))
            store.set("k4", 4)
            self.assertIsNone(store.get("k2"))
            self.assertIsNotNone(store.get("k1"))
            self.assertIsNotNone(store.get("k4"))
        finally:
            store.stop()

    def test_10_bounded_server_rejects_excess_concurrency(self) -> None:
        srv = BoundedThreadingHTTPServer(("127.0.0.1", 0), _SlowHandler, max_concurrent_requests=1)
        port = srv.server_address[1]
        import threading
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            def _hit(_: int) -> int:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2.0) as resp:
                        return resp.status
                except urllib.error.HTTPError as exc:
                    return exc.code

            with ThreadPoolExecutor(max_workers=4) as pool:
                statuses = list(pool.map(_hit, range(4)))
            self.assertIn(200, statuses)
            self.assertIn(503, statuses)
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
