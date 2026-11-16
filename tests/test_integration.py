import time
import unittest

from tests.harness import http_request, start_test_cluster, stop_test_cluster


class TestMicroservicesIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls) -> None:
        stop_test_cluster()

    def test_01_all_services_healthy(self) -> None:
        for name, base_url in self.urls.items():
            status, data = http_request("GET", f"{base_url}/health", timeout=2.0)
            self.assertEqual(status, 200, f"{name} health check returned {status}")
            self.assertEqual(data.get("status"), "healthy")

    def test_02_frontend_to_gateway_aggregation(self) -> None:
        status, data = http_request("GET", f"{self.urls['frontend']}/api/status", timeout=3.0)
        self.assertEqual(status, 200)
        self.assertEqual(data.get("gateway"), "healthy")
        self.assertTrue(data.get("system_healthy"))
        self.assertEqual(data["dependencies"]["backend_worker"]["status_code"], 200)
        self.assertEqual(data["dependencies"]["data_cache_store"]["status_code"], 200)

    def test_03_job_submission_and_lifecycle(self) -> None:
        payload = {"payload": "Distributed systems reliability test.", "simulate_delay": 0.1}
        status, data = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload, timeout=3.0)
        self.assertEqual(status, 202)
        job_id = data["job_id"]

        deadline = time.monotonic() + 3.0
        final_job = None
        while time.monotonic() < deadline:
            poll_status, poll_data = http_request("GET", f"{self.urls['gateway']}/api/jobs/{job_id}", timeout=2.0)
            self.assertEqual(poll_status, 200)
            if poll_data.get("status") in ("COMPLETED", "FAILED"):
                final_job = poll_data
                break
            time.sleep(0.05)

        self.assertIsNotNone(final_job)
        self.assertEqual(final_job["status"], "COMPLETED")
        self.assertIn("sha256", final_job["result"])
        self.assertGreater(final_job["result"]["word_count"], 0)

    def test_04_cache_hit_on_duplicate_payload(self) -> None:
        payload = {"payload": "Deterministic payload for cache hit verification", "simulate_delay": 0.15}
        _, run1 = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        time.sleep(0.25)
        _, job1 = http_request("GET", f"{self.urls['gateway']}/api/jobs/{run1['job_id']}")
        self.assertEqual(job1["status"], "COMPLETED")
        self.assertFalse(job1["cache_hit"])

        _, run2 = http_request("POST", f"{self.urls['gateway']}/api/jobs", payload=payload)
        time.sleep(0.1)
        _, job2 = http_request("GET", f"{self.urls['gateway']}/api/jobs/{run2['job_id']}")
        self.assertEqual(job2["status"], "COMPLETED")
        self.assertTrue(job2["cache_hit"])
        self.assertEqual(job1["result"]["sha256"], job2["result"]["sha256"])

    def test_05_cache_ttl_expiration(self) -> None:
        test_key = "ttl_test_key_001"
        status, _ = http_request(
            "POST",
            f"{self.urls['cache']}/cache",
            payload={"key": test_key, "value": {"test": "data"}, "ttl_seconds": 0.4},
        )
        self.assertEqual(status, 201)

        status, data = http_request("GET", f"{self.urls['cache']}/cache/{test_key}")
        self.assertEqual(status, 200)
        self.assertEqual(data.get("status"), "hit")

        time.sleep(0.55)
        status, data = http_request("GET", f"{self.urls['cache']}/cache/{test_key}")
        self.assertEqual(status, 404)
        self.assertEqual(data.get("status"), "miss")


if __name__ == "__main__":
    unittest.main()
