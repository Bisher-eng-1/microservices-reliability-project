# Microservices Reliability Project

Four decoupled Python HTTP services communicating over an internal Docker bridge network, instrumented with Prometheus `/metrics` endpoints and cAdvisor container metrics.

## Architecture

```text
Browser -> frontend-service (:8080) -> api-gateway (:8000) -> backend-worker (:8001)
                                            |                        |
                                            +-----> data-cache-store (:8002)
```

- `frontend-service` (`:8080`): serves a control UI and proxies `/api/*` requests to the gateway.
- `api-gateway` (`:8000`): validates payloads, forwards `X-Request-Id`, aggregates dependency health, and isolates downstream failures with per-target circuit breakers.
- `backend-worker` (`:8001`): runs asynchronous text-analysis jobs in a `ThreadPoolExecutor`, reads/writes cached results by SHA-256 payload key, and caps retained job history in memory.
- `data-cache-store` (`:8002`): in-memory LRU key-value store with TTL expiration and a background eviction thread.

## Reliability & Observability Mechanics

- **Single-probe half-open circuit breaker**: after `failure_threshold` (`3`) downstream 5xx/network errors, the circuit opens for `cooldown_seconds` (`8.0`s) using monotonic clocks. When the cooldown expires, only one concurrent probe request passes in `HALF_OPEN`; any probe failure immediately re-opens the circuit.
- **Bounded request concurrency**: all services run on `BoundedThreadingHTTPServer` (`max_concurrent_requests=64`), returning HTTP `503` instead of spawning unbounded OS threads under burst load.
- **Bounded memory**: `data-cache-store` caps keys via LRU eviction (`MAX_CACHE_ENTRIES=10000`) and `backend-worker` caps completed job retention (`MAX_RETAINED_JOBS=1000`).
- **Metrics**: each service exposes `/metrics` (`http_requests_total`, `http_request_duration_seconds`, plus service-specific counters/gauges); `docker-compose.yml` wires Prometheus (`:9090`) and cAdvisor (`:8085`).

## Running Locally

Start all containers:

```bash
docker compose up --build
```

Run the live-socket integration and reliability test suite locally (stdlib `unittest` or `pytest`):

```bash
pytest -v tests/
# or without extra dependencies:
python3 -m unittest discover -s tests -v
```

Lint check:

```bash
flake8 services tests --select=E9,F63,F7,F82 --show-source
```

## Limitations

- Job state (`backend-worker`) and cached entries (`data-cache-store`) live strictly in process memory and are lost on container restart.
- `ThreadPoolExecutor` queues jobs per worker instance; horizontal worker scaling would require an external broker (e.g., RabbitMQ or Redis Streams).
- `cadvisor` in `docker-compose.yml` targets Linux hosts with `/dev/kmsg` and cgroup mounts.
