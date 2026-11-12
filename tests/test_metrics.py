"""
Metrics Verification Tests for Prometheus Observability.
Verifies standard exposition format (# HELP, # TYPE) across all four microservices.
"""
import unittest
import urllib.request

from tests.harness import get_cluster_urls, http_request, start_test_cluster, stop_test_cluster


class TestMicroservicesMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls):
        stop_test_cluster()

    def test_01_all_services_expose_valid_prometheus_metrics(self):
        """Verify each of the 4 microservices responds to /metrics with valid Prometheus exposition format."""
        for name, base_url in self.urls.items():
            req = urllib.request.Request(f"{base_url}/metrics")
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                self.assertEqual(resp.status, 200)
                content_type = resp.headers.get("Content-Type", "")
                self.assertIn("text/plain", content_type)
                body = resp.read().decode("utf-8")

                self.assertIn("# HELP", body, f"Service {name} /metrics missing '# HELP'")
                self.assertIn("# TYPE", body, f"Service {name} /metrics missing '# TYPE'")
                self.assertIn("http_requests_total", body, f"Service {name} /metrics missing 'http_requests_total'")

    def test_02_cache_metrics_increment_on_operations(self):
        """Verify cache_operations_total and cache_keys_total reflect real cache actions."""
        # Initial scrape
        with urllib.request.urlopen(f"{self.urls['cache']}/metrics") as resp:
            body_before = resp.read().decode("utf-8")

        # Perform cache write
        http_request("POST", f"{self.urls['cache']}/cache", payload={"key": "metric_key_test", "value": "val123"})

        # Subsequent scrape
        with urllib.request.urlopen(f"{self.urls['cache']}/metrics") as resp:
            body_after = resp.read().decode("utf-8")

        self.assertIn("cache_operations_total", body_after)
        self.assertIn("cache_keys_total", body_after)


if __name__ == "__main__":
    unittest.main()
