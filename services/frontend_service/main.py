"""
Service B: frontend-service
Lightweight Web presentation service.
Serves a clean HTML/JS dashboard, proxies requests through the API Gateway,
and exposes Prometheus metrics and health checks.
"""
import http.server
import json
import logging
import os
import sys
import time
from typing import Any, Dict
from urllib.parse import urlparse

# Add workspace root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.metrics import MetricsRegistry

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","service":"frontend-service","message":"%(message)s"}',
)
logger = logging.getLogger("frontend-service")

SERVICE_NAME = "frontend-service"
PORT = get_env_int("PORT", 8080)
HOST = get_env_str("HOST", "0.0.0.0")
GATEWAY_URL = get_env_str("GATEWAY_URL", "http://api-gateway:8000")
GATEWAY_TIMEOUT = get_env_float("GATEWAY_TIMEOUT_SECONDS", 3.0)

# Observability Metrics Registry
metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter(
    "http_requests_total", "Total count of HTTP requests processed by frontend-service"
)
HTTP_DURATION = metrics_reg.register_histogram(
    "http_request_duration_seconds", "HTTP request latency in seconds for frontend-service"
)
GATEWAY_FORWARD_REQUESTS = metrics_reg.register_counter(
    "frontend_gateway_requests_total", "Requests forwarded from frontend-service to api-gateway"
)
GATEWAY_FORWARD_FAILURES = metrics_reg.register_counter(
    "frontend_gateway_failures_total", "Failures encountered contacting api-gateway"
)

# Resilient Client contacting API Gateway
gateway_client = ResilientHttpClient("api-gateway", default_timeout_seconds=GATEWAY_TIMEOUT)

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Microservices Reliability Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --success: #22c55e;
      --danger: #ef4444;
      --warning: #f59e0b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      padding: 24px;
      line-height: 1.5;
    }
    .container { max-width: 1200px; margin: 0 auto; }
    header {
      margin-bottom: 24px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
    }
    h1 { font-size: 1.5rem; font-weight: 700; color: #fff; }
    .subtitle { font-size: 0.875rem; color: var(--text-muted); }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 0.75rem;
      font-weight: 600;
      background: rgba(56, 189, 248, 0.1);
      color: var(--accent);
      border: 1px solid rgba(56, 189, 248, 0.3);
    }
    .badge-success { background: rgba(34, 197, 94, 0.15); color: var(--success); border-color: rgba(34, 197, 94, 0.3); }
    .badge-danger { background: rgba(239, 68, 68, 0.15); color: var(--danger); border-color: rgba(239, 68, 68, 0.3); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 16px;
    }
    .card-title { font-size: 0.95rem; font-weight: 600; margin-bottom: 8px; display: flex; justify-content: space-between; }
    .btn {
      background: #0284c7;
      color: #fff;
      border: none;
      border-radius: 6px;
      padding: 8px 16px;
      font-size: 0.875rem;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.15s;
    }
    .btn:hover { background: #0369a1; }
    .btn-danger { background: #dc2626; }
    .btn-danger:hover { background: #b91c1c; }
    .form-group { margin-bottom: 12px; }
    label { display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 4px; }
    input, textarea, select {
      width: 100%;
      background: #0f172a;
      border: 1px solid var(--border);
      border-radius: 6px;
      color: #fff;
      padding: 8px 12px;
      font-size: 0.875rem;
    }
    input:focus, textarea:focus { outline: 1px solid var(--accent); }
    pre {
      background: #090d16;
      border: 1px solid #1e293b;
      padding: 12px;
      border-radius: 6px;
      font-size: 0.75rem;
      overflow-x: auto;
      max-height: 250px;
      color: #cbd5e1;
    }
    .arch-flow {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 12px;
      background: #090d16;
      border-radius: 6px;
      margin-bottom: 20px;
      overflow-x: auto;
      font-size: 0.85rem;
    }
    .node {
      background: var(--card-bg);
      border: 1px solid var(--border);
      padding: 6px 12px;
      border-radius: 6px;
      font-weight: 600;
    }
    .arrow { color: var(--accent); font-weight: bold; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>Microservices Reliability Architecture</h1>
        <div class="subtitle">Four decoupled services communicating via Docker internal network</div>
      </div>
      <div>
        <span id="gateway-status" class="badge">Connecting to Gateway...</span>
        <button class="btn" onclick="checkStatus()" style="margin-left: 8px;">Refresh</button>
      </div>
    </header>

    <div class="arch-flow">
      <div class="node">Browser</div>
      <div class="arrow">&rarr;</div>
      <div class="node" style="border-color: #38bdf8;">frontend-service (:8080)</div>
      <div class="arrow">&rarr;</div>
      <div class="node" style="border-color: #818cf8;">api-gateway (:8000)</div>
      <div class="arrow">&rarr;</div>
      <div class="node" style="border-color: #34d399;">backend-worker (:8001)</div>
      <div class="arrow">&rarr;</div>
      <div class="node" style="border-color: #fbbf24;">data-cache-store (:8002)</div>
    </div>

    <div class="grid">
      <!-- Card: Submit Job -->
      <div class="card">
        <div class="card-title">
          <span>Submit Processing Job</span>
          <span class="badge">Worker Queue</span>
        </div>
        <div class="form-group">
          <label>Payload Text</label>
          <textarea id="job-payload" rows="3">Microservices architecture decouples responsibilities and isolates failures.</textarea>
        </div>
        <div class="form-group" style="display: flex; gap: 8px;">
          <div style="flex: 1;">
            <label>Delay (seconds)</label>
            <input type="number" id="job-delay" value="0.2" step="0.1" min="0" max="5">
          </div>
          <div style="flex: 1;">
            <label>Simulate Failure</label>
            <select id="job-fail">
              <option value="false">Normal (Success)</option>
              <option value="true">Force Failure</option>
            </select>
          </div>
        </div>
        <button class="btn" style="width: 100%;" onclick="submitJob()">Submit via Gateway</button>
      </div>

      <!-- Card: System Status -->
      <div class="card">
        <div class="card-title">
          <span>Decoupled Service Health</span>
          <span class="badge" id="agg-badge">Gateway Aggregated</span>
        </div>
        <div id="service-health-list" style="font-size: 0.85rem; display: flex; flex-direction: column; gap: 8px; margin-top: 8px;">
          <div>Checking downstream services...</div>
        </div>
      </div>
    </div>

    <!-- Active Jobs & Cache Inspector -->
    <div class="grid">
      <div class="card">
        <div class="card-title">
          <span>Recent Worker Jobs</span>
          <button class="btn" style="padding: 2px 8px; font-size: 0.75rem;" onclick="loadJobs()">Poll Jobs</button>
        </div>
        <pre id="jobs-output">Loading jobs...</pre>
      </div>

      <div class="card">
        <div class="card-title">
          <span>API Gateway Response</span>
          <span class="badge">Audit Log</span>
        </div>
        <pre id="api-output">Ready for requests.</pre>
      </div>
    </div>
  </div>

  <script>
    async function checkStatus() {
      const out = document.getElementById('api-output');
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        const badge = document.getElementById('gateway-status');
        if (res.ok) {
          badge.className = 'badge badge-success';
          badge.innerText = 'Gateway Connected (OK)';
        } else {
          badge.className = 'badge badge-danger';
          badge.innerText = 'Degraded (' + res.status + ')';
        }
        
        const list = document.getElementById('service-health-list');
        list.innerHTML = `
          <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #334155;">
            <span>frontend-service</span> <span class="badge badge-success">OK (:8080)</span>
          </div>
          <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #334155;">
            <span>api-gateway</span> <span class="badge badge-success">OK (:8000)</span>
          </div>
          <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #334155;">
            <span>backend-worker</span> <span class="badge ${data.dependencies?.backend_worker?.status_code === 200 ? 'badge-success' : 'badge-danger'}">${data.dependencies?.backend_worker?.status_code === 200 ? 'Healthy' : 'Unavailable'}</span>
          </div>
          <div style="display: flex; justify-content: space-between; padding: 4px 0;">
            <span>data-cache-store</span> <span class="badge ${data.dependencies?.data_cache_store?.status_code === 200 ? 'badge-success' : 'badge-danger'}">${data.dependencies?.data_cache_store?.status_code === 200 ? 'Healthy' : 'Unavailable'}</span>
          </div>
        `;
        out.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        document.getElementById('gateway-status').className = 'badge badge-danger';
        document.getElementById('gateway-status').innerText = 'Gateway Unreachable';
        out.innerText = 'Error connecting to gateway: ' + e.message;
      }
    }

    async function submitJob() {
      const payload = document.getElementById('job-payload').value;
      const delay = parseFloat(document.getElementById('job-delay').value || '0');
      const fail = document.getElementById('job-fail').value === 'true';
      const out = document.getElementById('api-output');

      out.innerText = 'Submitting job through API Gateway...';
      try {
        const res = await fetch('/api/jobs', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            payload: payload,
            job_type: 'text_analysis',
            simulate_delay: delay,
            force_fail: fail
          })
        });
        const data = await res.json();
        out.innerText = JSON.stringify(data, null, 2);
        setTimeout(loadJobs, 300);
      } catch (e) {
        out.innerText = 'Error submitting job: ' + e.message;
      }
    }

    async function loadJobs() {
      const pre = document.getElementById('jobs-output');
      try {
        const res = await fetch('/api/jobs');
        const data = await res.json();
        pre.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        pre.innerText = 'Failed to load jobs: ' + e.message;
      }
    }

    // Initial check
    checkStatus();
    loadJobs();
  </script>
</body>
</html>
"""


class FrontendRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, data: Dict[str, Any]) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        start_time = time.time()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200

        try:
            if path in ("", "/"):
                self._send_html(HTML_DASHBOARD)
                return

            if path == "/health":
                # Check gateway health
                g_status, _ = gateway_client.request("GET", f"{GATEWAY_URL}/health", timeout=1.0)
                self._send_json(200, {
                    "status": "healthy",
                    "service": SERVICE_NAME,
                    "gateway_connection": "healthy" if g_status == 200 else "degraded",
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

            # Proxy /api/* to API Gateway
            if path.startswith("/api"):
                GATEWAY_FORWARD_REQUESTS.inc()
                target_url = f"{GATEWAY_URL}{parsed.path}"
                if parsed.query:
                    target_url += f"?{parsed.query}"

                g_status, g_data = gateway_client.request("GET", target_url)
                if g_status >= 500:
                    GATEWAY_FORWARD_FAILURES.inc()
                status = g_status
                self._send_json(status, g_data)
                return

            status = 404
            self._send_json(404, {"error": "Page not found on frontend-service"})

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
            if path.startswith("/api"):
                content_length = int(self.headers.get("Content-Length", 0))
                raw_body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
                try:
                    body_json = json.loads(raw_body)
                except Exception:
                    body_json = {}

                GATEWAY_FORWARD_REQUESTS.inc()
                target_url = f"{GATEWAY_URL}{parsed.path}"
                g_status, g_data = gateway_client.request("POST", target_url, payload=body_json)
                if g_status >= 500:
                    GATEWAY_FORWARD_FAILURES.inc()

                status = g_status
                self._send_json(status, g_data)
                return

            status = 404
            self._send_json(404, {"error": "Endpoint not found"})

        finally:
            duration = time.time() - start_time
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(duration, labels={"method": "POST"})


SERVER_START_TIME = time.time()


def run_server(port: int = PORT, host: str = HOST) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer((host, port), FrontendRequestHandler)
    logger.info("Service B [frontend-service] listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    server = run_server()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down frontend-service...")
        server.shutdown()
