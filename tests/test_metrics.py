import unittest
import urllib.request

from tests.harness import http_request, start_test_cluster, stop_test_cluster


class TestMicroservicesMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.urls = start_test_cluster()

    @classmethod
    def tearDownClass(cls) -> None:
        stop_test_cluster()

    def test_01_all_services_expose_valid_prometheus_metrics(self) -> None:
        for name, base_url in self.urls.items():
            with urllib.request.urlopen(f"{base_url}/metrics", timeout=2.0) as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn("text/plain", resp.headers.get("Content-Type", ""))
                body = resp.read().decode("utf-8")
                self.assertIn("# HELP", body, f"{name} missing # HELP")
                self.assertIn("# TYPE", body, f"{name} missing # TYPE")
                self.assertIn("http_requests_total", body, f"{name} missing http_requests_total")

    def test_02_cache_metrics_increment_on_operations(self) -> None:
        http_request("POST", f"{self.urls['cache']}/cache", payload={"key": "metric_key_test", "value": "val123"})
        with urllib.request.urlopen(f"{self.urls['cache']}/metrics", timeout=2.0) as resp:
            body = resp.read().decode("utf-8")
        self.assertIn("cache_operations_total", body)
        self.assertIn("cache_keys_total", body)

    def test_03_gateway_status_emits_labeled_downstream_metrics(self) -> None:
        status, _ = http_request("GET", f"{self.urls['gateway']}/api/status", timeout=2.0)
        self.assertEqual(status, 200)
        with urllib.request.urlopen(f"{self.urls['gateway']}/metrics", timeout=2.0) as resp:
            body = resp.read().decode("utf-8")
        self.assertIn('downstream_requests_total{target="backend-worker"}', body)
        self.assertIn('downstream_requests_total{target="data-cache-store"}', body)
        for line in body.splitlines():
            self.assertFalse(line.startswith("downstream_requests_total "), f"Unlabeled series emitted: {line}")


if __name__ == "__main__":
    unittest.main()
