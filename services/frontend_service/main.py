import time
from urllib.parse import urlparse

from services.common.config import get_env_float, get_env_int, get_env_str
from services.common.http_client import ResilientHttpClient
from services.common.http_server import (
    BoundedThreadingHTTPServer,
    JsonRequestHandler,
    configure_service_logger,
)
from services.common.metrics import MetricsRegistry

SERVICE_NAME = "frontend-service"
logger = configure_service_logger(SERVICE_NAME)

PORT = get_env_int("PORT", 8080)
HOST = get_env_str("HOST", "0.0.0.0")
GATEWAY_URL = get_env_str("GATEWAY_URL", "http://api-gateway:8000")
GATEWAY_TIMEOUT = get_env_float("GATEWAY_TIMEOUT_SECONDS", 3.0)

metrics_reg = MetricsRegistry()
HTTP_REQUESTS = metrics_reg.register_counter("http_requests_total", "Total HTTP requests processed")
HTTP_DURATION = metrics_reg.register_histogram("http_request_duration_seconds", "HTTP request latency in seconds")
GATEWAY_FORWARD_REQUESTS = metrics_reg.register_counter(
    "frontend_gateway_requests_total", "Requests forwarded to api-gateway"
)
GATEWAY_FORWARD_FAILURES = metrics_reg.register_counter(
    "frontend_gateway_failures_total", "Failures contacting api-gateway"
)

gateway_client = ResilientHttpClient("api-gateway", default_timeout_seconds=GATEWAY_TIMEOUT)
SERVER_START_TIME = time.monotonic()

HTML_DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Microservices Status</title>
  <style>
    :root {
      --bg: #0f172a; --card: #1e293b; --border: #334155;
      --text: #f8fafc; --muted: #94a3b8; --accent: #38bdf8;
      --ok: #22c55e; --err: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg); color: var(--text); padding: 24px; line-height: 1.5;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    .container { max-width: 1100px; margin: 0 auto; }
    header {
      margin-bottom: 20px; border-bottom: 1px solid var(--border); padding-bottom: 14px;
      display: flex; justify-content: space-between; align-items: center; gap: 12px;
    }
    h1 { font-size: 1.4rem; }
    .subtitle { font-size: 0.85rem; color: var(--muted); }
    .badge {
      padding: 3px 10px; border-radius: 999px; font-size: 0.75rem; font-weight: 600;
      border: 1px solid rgba(56,189,248,0.3); color: var(--accent);
    }
    .badge-ok { background: rgba(34,197,94,0.15); color: var(--ok); border-color: rgba(34,197,94,0.3); }
    .badge-err { background: rgba(239,68,68,0.15); color: var(--err); border-color: rgba(239,68,68,0.3); }
    .grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px; margin-bottom: 20px;
    }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
    .card-title { font-size: 0.95rem; font-weight: 600; margin-bottom: 10px; display: flex; }
    .card-title span:first-child { flex: 1; }
    .btn {
      background: #0284c7; color: #fff; border: none; border-radius: 6px;
      padding: 8px 14px; font-size: 0.85rem; font-weight: 600; cursor: pointer;
    }
    label { display: block; font-size: 0.8rem; color: var(--muted); margin-bottom: 4px; }
    input, textarea, select {
      width: 100%; background: #0f172a; border: 1px solid var(--border);
      border-radius: 6px; color: #fff; padding: 8px 10px; font-size: 0.85rem;
    }
    .form-group { margin-bottom: 10px; }
    pre {
      background: #090d16; border: 1px solid var(--border); padding: 10px;
      border-radius: 6px; font-size: 0.75rem; overflow-x: auto; max-height: 240px;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>Microservices Control Panel</h1>
        <div class="subtitle">frontend-service -> api-gateway -> backend-worker -> data-cache-store</div>
      </div>
      <div>
        <span id="gw-badge" class="badge">Checking...</span>
        <button class="btn" onclick="checkStatus()">Refresh</button>
      </div>
    </header>
    <div class="grid">
      <div class="card">
        <div class="card-title"><span>Submit Job</span></div>
        <div class="form-group">
          <label>Payload Text</label>
          <textarea id="job-payload" rows="3">Distributed job payload for cache and worker check.</textarea>
        </div>
        <div class="form-group" style="display:flex;gap:8px;">
          <div style="flex:1;"><label>Delay (s)</label>
            <input type="number" id="job-delay" value="0.2" step="0.1" min="0" max="5">
          </div>
          <div style="flex:1;"><label>Mode</label>
            <select id="job-fail"><option value="false">Normal</option><option value="true">Force Fail</option></select>
          </div>
        </div>
        <button class="btn" style="width:100%;" onclick="submitJob()">Submit</button>
      </div>
      <div class="card">
        <div class="card-title"><span>Downstream Health</span></div>
        <div id="health-list" style="font-size:0.85rem;display:flex;flex-direction:column;gap:8px;"></div>
      </div>
    </div>
    <div class="grid">
      <div class="card">
        <div class="card-title"><span>Recent Jobs</span><button class="btn" onclick="loadJobs()">Poll</button></div>
        <pre id="jobs-output">Loading...</pre>
      </div>
      <div class="card">
        <div class="card-title"><span>Gateway Response</span></div>
        <pre id="api-output">Ready.</pre>
      </div>
    </div>
  </div>
  <script>
    async function checkStatus() {
      const out = document.getElementById('api-output');
      try {
        const res = await fetch('/api/status');
        const data = await res.json();
        const badge = document.getElementById('gw-badge');
        badge.className = res.ok ? 'badge badge-ok' : 'badge badge-err';
        badge.innerText = res.ok ? 'Gateway OK' : 'Degraded (' + res.status + ')';
        const wOk = data.dependencies?.backend_worker?.status_code === 200;
        const cOk = data.dependencies?.data_cache_store?.status_code === 200;
        document.getElementById('health-list').innerHTML =
          `<div>backend-worker: <b>${wOk ? 'Healthy' : 'Unavailable'}</b></div>` +
          `<div>data-cache-store: <b>${cOk ? 'Healthy' : 'Unavailable'}</b></div>`;
        out.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        document.getElementById('gw-badge').className = 'badge badge-err';
        document.getElementById('gw-badge').innerText = 'Unreachable';
        out.innerText = e.message;
      }
    }
    async function submitJob() {
      const res = await fetch('/api/jobs', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          payload: document.getElementById('job-payload').value,
          simulate_delay: parseFloat(document.getElementById('job-delay').value || '0'),
          force_fail: document.getElementById('job-fail').value === 'true'
        })
      });
      document.getElementById('api-output').innerText = JSON.stringify(await res.json(), null, 2);
      setTimeout(loadJobs, 300);
    }
    async function loadJobs() {
      const res = await fetch('/api/jobs');
      document.getElementById('jobs-output').innerText = JSON.stringify(await res.json(), null, 2);
    }
    checkStatus();
    loadJobs();
  </script>
</body>
</html>
"""


class FrontendRequestHandler(JsonRequestHandler):
    service_name = SERVICE_NAME

    def do_GET(self) -> None:
        start = time.monotonic()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200
        try:
            if path in ("", "/"):
                self._send_bytes(200, "text/html; charset=utf-8", HTML_DASHBOARD.encode("utf-8"))
                return
            if path == "/health":
                g_status, _ = gateway_client.request("GET", f"{GATEWAY_URL}/health", timeout=1.0)
                self.send_json(200, {
                    "status": "healthy",
                    "service": SERVICE_NAME,
                    "gateway_connection": "healthy" if g_status == 200 else "degraded",
                    "uptime_seconds": round(time.monotonic() - SERVER_START_TIME, 2),
                })
                return
            if path == "/metrics":
                self.send_metrics(metrics_reg.generate_exposition())
                return
            if path.startswith("/api"):
                GATEWAY_FORWARD_REQUESTS.inc()
                target_url = f"{GATEWAY_URL}{parsed.path}" + (f"?{parsed.query}" if parsed.query else "")
                g_status, g_data = gateway_client.request("GET", target_url)
                if g_status >= 500:
                    GATEWAY_FORWARD_FAILURES.inc()
                status = g_status
                self.send_json(status, g_data)
                return
            status = 404
            self.send_json(404, {"error": "Page not found on frontend-service", "service": SERVICE_NAME})
        finally:
            HTTP_REQUESTS.inc(labels={"method": "GET", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "GET"})

    def do_POST(self) -> None:
        start = time.monotonic()
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        status = 200
        try:
            if not path.startswith("/api"):
                status = 404
                self.send_json(404, {"error": "Endpoint not found", "service": SERVICE_NAME})
                return
            body_json, err = self.read_json_body(max_bytes=256_000)
            if err is not None:
                status, body = err
                self.send_json(status, body)
                return
            GATEWAY_FORWARD_REQUESTS.inc()
            g_status, g_data = gateway_client.request("POST", f"{GATEWAY_URL}{parsed.path}", payload=body_json)
            if g_status >= 500:
                GATEWAY_FORWARD_FAILURES.inc()
            status = g_status
            self.send_json(status, g_data)
        finally:
            HTTP_REQUESTS.inc(labels={"method": "POST", "status": str(status)})
            HTTP_DURATION.observe(time.monotonic() - start, labels={"method": "POST"})


def run_server(port: int = PORT, host: str = HOST) -> BoundedThreadingHTTPServer:
    server = BoundedThreadingHTTPServer((host, port), FrontendRequestHandler)
    logger.info("frontend-service listening on http://%s:%d", host, port)
    return server


if __name__ == "__main__":
    srv = run_server()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down frontend-service")
        srv.shutdown()
