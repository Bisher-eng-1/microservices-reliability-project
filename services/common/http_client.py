"""
Resilient HTTP Client with Finite Timeouts and Circuit Breaker.
Guarantees:
- Never blocks indefinitely (strict socket connect & read timeouts).
- Returns structured JSON failure envelopes instead of uncaught exceptions.
- Implements Circuit Breaker to prevent cascading failures.
"""
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class CircuitBreakerOpenException(Exception):
    """Raised when the circuit breaker is in OPEN state."""
    pass


class CircuitBreaker:
    """
    Finite-state machine circuit breaker:
    - CLOSED: Requests pass normally. Failures increment failure counter.
    - OPEN: Requests fail immediately without hitting the downstream service.
    - HALF-OPEN: After cooldown_seconds, allows a single probe request to verify recovery.
    """
    STATE_CLOSED = "CLOSED"
    STATE_OPEN = "OPEN"
    STATE_HALF_OPEN = "HALF_OPEN"

    def __init__(self, name: str, failure_threshold: int = 3, cooldown_seconds: float = 10.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.state = self.STATE_CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.lock = threading.Lock()

    def allow_request(self) -> bool:
        with self.lock:
            now = time.time()
            if self.state == self.STATE_CLOSED:
                return True
            if self.state == self.STATE_OPEN:
                if now - self.last_failure_time > self.cooldown_seconds:
                    logger.info("CircuitBreaker [%s] transitioned from OPEN to HALF_OPEN", self.name)
                    self.state = self.STATE_HALF_OPEN
                    return True
                return False
            # HALF_OPEN allows probe
            return True

    def record_success(self) -> None:
        with self.lock:
            if self.state != self.STATE_CLOSED:
                logger.info("CircuitBreaker [%s] recovered -> transitioned to CLOSED", self.name)
            self.state = self.STATE_CLOSED
            self.failure_count = 0

    def record_failure(self) -> None:
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                if self.state != self.STATE_OPEN:
                    logger.warning(
                        "CircuitBreaker [%s] threshold reached (%d) -> transitioned to OPEN",
                        self.name,
                        self.failure_count,
                    )
                self.state = self.STATE_OPEN

    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "name": self.name,
                "state": self.state,
                "failure_count": self.failure_count,
                "threshold": self.failure_threshold,
                "last_failure_seconds_ago": round(time.time() - self.last_failure_time, 2)
                if self.last_failure_time > 0
                else None,
            }


class ResilientHttpClient:
    def __init__(self, target_service_name: str, default_timeout_seconds: float = 3.0):
        self.service_name = target_service_name
        self.timeout = default_timeout_seconds
        self.circuit_breaker = CircuitBreaker(target_service_name, failure_threshold=3, cooldown_seconds=8.0)

    def request(
        self,
        method: str,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        """
        Executes an HTTP request with finite timeout and circuit breaking.
        Returns: (status_code, response_data_dict)
        Guaranteed to not raise unhandled socket/connection exceptions.
        """
        effective_timeout = timeout or self.timeout
        req_headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if headers:
            req_headers.update(headers)

        if not self.circuit_breaker.allow_request():
            logger.warning("Downstream request blocked by Circuit Breaker for [%s]", self.service_name)
            return 503, {
                "error": f"Circuit breaker OPEN for downstream service '{self.service_name}'",
                "service": self.service_name,
                "circuit_breaker": self.circuit_breaker.get_status(),
                "transient": True,
            }

        data_bytes = None
        if payload is not None:
            data_bytes = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(url, data=data_bytes, headers=req_headers, method=method.upper())

        try:
            with urllib.request.urlopen(req, timeout=effective_timeout) as response:
                status = response.status
                body = response.read().decode("utf-8")
                self.circuit_breaker.record_success()
                try:
                    data = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    data = {"raw_response": body}
                return status, data

        except urllib.error.HTTPError as e:
            # Downstream responded with 4xx or 5xx HTTP status
            body = e.read().decode("utf-8")
            try:
                data = json.loads(body)
            except Exception:
                data = {"error": f"HTTP {e.code}: {e.reason}", "raw": body}

            # 5xx counts as downstream failure for circuit breaker, 4xx does not (client error)
            if e.code >= 500:
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success()

            return e.code, data

        except urllib.error.URLError as e:
            # Connection refused, network unreachable, or DNS failure
            self.circuit_breaker.record_failure()
            logger.error("Connection failed to [%s] at %s: %s", self.service_name, url, str(e.reason))
            return 503, {
                "error": f"Cannot connect to downstream service '{self.service_name}'",
                "service": self.service_name,
                "reason": str(e.reason),
                "url": url,
                "transient": True,
            }

        except TimeoutError:
            self.circuit_breaker.record_failure()
            logger.error("Timeout (%.2fs) contacting downstream [%s] at %s", effective_timeout, self.service_name, url)
            return 504, {
                "error": f"Timeout waiting for downstream service '{self.service_name}' after {effective_timeout}s",
                "service": self.service_name,
                "timeout_seconds": effective_timeout,
                "url": url,
                "transient": True,
            }

        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.exception("Unexpected error communicating with [%s]: %s", self.service_name, str(e))
            return 500, {
                "error": f"Internal communication error with '{self.service_name}'",
                "service": self.service_name,
                "details": str(e),
                "transient": True,
            }
