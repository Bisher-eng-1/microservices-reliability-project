import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class CircuitBreakerOpenException(Exception):
    pass


class CircuitBreaker:
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
        self._probe_in_flight = False
        self.lock = threading.Lock()

    def allow_request(self) -> bool:
        with self.lock:
            if self.state == self.STATE_CLOSED:
                return True
            if self.state == self.STATE_OPEN:
                if time.monotonic() - self.last_failure_time >= self.cooldown_seconds:
                    logger.info("CircuitBreaker [%s] OPEN -> HALF_OPEN", self.name)
                    self.state = self.STATE_HALF_OPEN
                    self._probe_in_flight = True
                    return True
                return False
            if self._probe_in_flight:
                return False
            self._probe_in_flight = True
            return True

    def record_success(self) -> None:
        with self.lock:
            if self.state != self.STATE_CLOSED:
                logger.info("CircuitBreaker [%s] %s -> CLOSED", self.name, self.state)
            self.state = self.STATE_CLOSED
            self.failure_count = 0
            self._probe_in_flight = False

    def record_failure(self) -> None:
        with self.lock:
            now = time.monotonic()
            self.failure_count += 1
            self.last_failure_time = now
            self._probe_in_flight = False
            if self.state == self.STATE_HALF_OPEN or self.failure_count >= self.failure_threshold:
                if self.state != self.STATE_OPEN:
                    logger.warning(
                        "CircuitBreaker [%s] %s -> OPEN (failures=%d)",
                        self.name,
                        self.state,
                        self.failure_count,
                    )
                self.state = self.STATE_OPEN

    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            elapsed = (
                round(time.monotonic() - self.last_failure_time, 2)
                if self.last_failure_time > 0
                else None
            )
            return {
                "name": self.name,
                "state": self.state,
                "failure_count": self.failure_count,
                "threshold": self.failure_threshold,
                "last_failure_seconds_ago": elapsed,
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
        effective_timeout = timeout if timeout is not None else self.timeout
        req_headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if headers:
            req_headers.update(headers)

        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker blocked call to [%s]", self.service_name)
            return 503, {
                "error": f"Circuit breaker OPEN for downstream service '{self.service_name}'",
                "service": self.service_name,
                "circuit_breaker": self.circuit_breaker.get_status(),
                "transient": True,
            }

        data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
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
            body = e.read().decode("utf-8")
            try:
                data = json.loads(body)
            except Exception:
                data = {"error": f"HTTP {e.code}: {e.reason}", "raw": body}
            if e.code >= 500:
                self.circuit_breaker.record_failure()
            else:
                self.circuit_breaker.record_success()
            return e.code, data
        except urllib.error.URLError as e:
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
            logger.error("Timeout (%.2fs) calling [%s] at %s", effective_timeout, self.service_name, url)
            return 504, {
                "error": f"Timeout waiting for downstream service '{self.service_name}' after {effective_timeout}s",
                "service": self.service_name,
                "timeout_seconds": effective_timeout,
                "url": url,
                "transient": True,
            }
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.exception("Unexpected error calling [%s]: %s", self.service_name, str(e))
            return 500, {
                "error": f"Internal communication error with '{self.service_name}'",
                "service": self.service_name,
                "details": str(e),
                "transient": True,
            }
