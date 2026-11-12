"""
Reliability and Failure Injection Tests for Microservices Architecture.
Verifies failure isolation, circuit breakers, timeout enforcement,
and input validation without unhandled exceptions.
"""
from concurrent.futures import ThreadPoolExecutor
import time
import unittest

from services.common.http_client import CircuitBreaker, ResilientHttpClient
from tests.harness import get_cluster_urls, http_request, start_test_cluster, stop_test_cluster


class TestMicroservicesReliability(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls):
        stop_test_cluster()

    def test_01_invalid_payload_rejection(self):
        """Verify API Gateway rejects requests missing required 'payload' property with 400 Bad Request."""
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload={"invalid_field": 123})
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        self.assertEqual(data.get("service"), "api-gateway")

    def test_02_unknown_job_id_returns_404(self):
        """Verify non-existent job queries return a clean 404 rather than 500 error."""
        status, data = http_request("GET", f"{self.urls['gateway']}/api/jobs/job-nonexistent-99999")
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    def test_03_forced_worker_failure_handling(self):
        """Verify worker handles simulated runtime exceptions gracefully, updating job status to FAILED."""
        payload = {"payload": "Crash test", "simulate_delay": 0.05, "force_fail": True}
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        self.assertEqual(status, 202)
        job_id = data["job_id"]

        time.sleep(0.25)
        status, job = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(job["status"], "FAILED")
        self.assertIn("error", job)
        self.assertIn("Simulated worker processing failure", job["error"])

    def test_04_circuit_breaker_transitions(self):
        """Verify Circuit Breaker transitions: CLOSED -> OPEN after 3 failures -> rejects calls fast."""
        cb = CircuitBreaker("test-service", failure_threshold=3, cooldown_seconds=0.5)
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        self.assertTrue(cb.allow_request())

        # Simulate 3 failures
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreaker.STATE_OPEN)

        # In OPEN state, request is immediately rejected without touching network
        self.assertFalse(cb.allow_request())

        # Wait for cooldown
        time.sleep(0.6)
        # Should transition to HALF_OPEN to allow single probe
        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitBreaker.STATE_HALF_OPEN)

        # Successful probe resets to CLOSED
        cb.record_success()
        self.assertEqual(cb.state, CircuitBreaker.STATE_CLOSED)
        self.assertEqual(cb.failure_count, 0)

    def test_05_unreachable_downstream_handling(self):
        """Verify client communicating with an unreachable port returns structured 503 instead of crashing."""
        client = ResilientHttpClient("ghost-service", default_timeout_seconds=0.5)
        status, data = client.request("GET", "http://127.0.0.1:54321/ghost")
        self.assertEqual(status, 503)
        self.assertIn("error", data)
        self.assertTrue(data.get("transient"))

    def test_06_concurrent_job_submissions(self):
        """Verify thread safety and queue stability under concurrent job submissions."""
        concurrency = 10
        payloads = [{"payload": f"Concurrent task {i}", "simulate_delay": 0.05} for i in range(concurrency)]

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(http_request, "POST", f"{self.urls['gateway']}/api/jobs", p)
                for p in payloads
            ]
            results = [f.result() for f in futures]

        for status, data in results:
            self.assertEqual(status, 202)
            self.assertIn("job_id", data)


if __name__ == "__main__":
    unittest.main()
