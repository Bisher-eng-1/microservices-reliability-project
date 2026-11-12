"""
Integration Tests for Microservices Reliability Project.
Verifies real HTTP communication across decoupled services:
frontend -> api-gateway -> backend-worker -> data-cache-store
"""
import time
import unittest

from tests.harness import get_cluster_urls, http_request, start_test_cluster, stop_test_cluster


class TestMicroservicesIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls):
        stop_test_cluster()

    def test_01_all_services_healthy(self):
        """Verify all 4 microservices report healthy status on their /health endpoints."""
        for name, base_url in self.urls.items():
            status, data = http_request("GET", f"{base_url}/health", timeout=2.0)
            self.assertEqual(status, 200, f"Service {name} healthcheck failed with status {status}")
            self.assertEqual(data.get("status"), "healthy", f"Service {name} did not return status='healthy'")

    def test_02_frontend_to_gateway_aggregation(self):
        """Verify frontend-service proxies status checks through api-gateway."""
        status, data = http_request("GET", f"{self.urls['frontend']}/api/status", timeout=3.0)
        self.assertEqual(status, 200)
        self.assertEqual(data.get("gateway"), "healthy")
        self.assertTrue(data.get("system_healthy"), "Downstream services should be healthy")
        self.assertEqual(data["dependencies"]["backend_worker"]["status_code"], 200)
        self.assertEqual(data["dependencies"]["data_cache_store"]["status_code"], 200)

    def test_03_job_submission_and_lifecycle(self):
        """Verify submitting a job through the gateway, polling worker until completion, and inspecting result."""
        payload = {"payload": "Distributed systems reliability and failure containment test.", "simulate_delay": 0.1}
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload, timeout=3.0)

        self.assertEqual(status, 202, f"Gateway did not return 202 Accepted: {data}")
        self.assertIn("job_id", data)
        job_id = data["job_id"]

        # Poll with finite deadline (3 seconds max)
        deadline = time.time() + 3.0
        final_job = None
        while time.time() < deadline:
            poll_status, poll_data = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id}", timeout=2.0)
            self.assertEqual(poll_status, 200)
            if poll_data.get("status") in ("COMPLETED", "FAILED"):
                final_job = poll_data
                break
            time.sleep(0.05)

        self.assertIsNotNone(final_job, f"Job {job_id} did not finish within deadline")
        self.assertEqual(final_job["status"], "COMPLETED")
        self.assertIn("result", final_job)
        self.assertIn("sha256", final_job["result"])
        self.assertGreater(final_job["result"]["word_count"], 0)

    def test_04_cache_hit_on_duplicate_payload(self):
        """Verify identical payload hits the cache store, bypassing re-computation."""
        payload = {"payload": "Deterministic payload for cache hit verification", "simulate_delay": 0.2}

        # First run: Cache miss -> processes and caches
        _, run1 = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        job_id_1 = run1["job_id"]

        # Wait for completion
        time.sleep(0.3)
        _, job1 = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id_1}")
        self.assertEqual(job1["status"], "COMPLETED")
        self.assertFalse(job1["cache_hit"])

        # Second run: Identical payload -> Should immediately hit cache
        _, run2 = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        job_id_2 = run2["job_id"]

        time.sleep(0.1)
        _, job2 = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id_2}")
        self.assertEqual(job2["status"], "COMPLETED")
        self.assertTrue(job2["cache_hit"], "Second job with identical payload must hit cache")
        self.assertEqual(job1["result"]["sha256"], job2["result"]["sha256"])

    def test_05_cache_ttl_expiration(self):
        """Verify cache key expiration after TTL window passes."""
        test_key = "ttl_test_key_001"
        payload = {"key": test_key, "value": {"test": "data"}, "ttl_seconds": 0.5}

        # Store with 0.5s TTL
        status, _ = http_request("POST", f"{self.urls['cache']}/cache", payload=payload)
        self.assertEqual(status, 201)

        # Immediate check -> Cache Hit
        status, data = http_request("GET", f"{self.urls['cache']}/cache/{test_key}")
        self.assertEqual(status, 200)
        self.assertEqual(data.get("status"), "hit")

        # Wait for expiration
        time.sleep(0.7)

        # Post-expiry check -> Cache Miss (404)
        status, data = http_request("GET", f"{self.urls['cache']}/cache/{test_key}")
        self.assertEqual(status, 404)
        self.assertEqual(data.get("status"), "miss")


if __name__ == "__main__":
    unittest.main()
