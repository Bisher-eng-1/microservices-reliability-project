import http.server
import json
import logging
import threading
from typing import Any, Dict, Optional, Tuple, Type


def configure_service_logger(service_name: str) -> logging.Logger:
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
                f'"service":"{service_name}","message":"%(message)s"}}'
            )
        )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


class BoundedThreadingHTTPServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        RequestHandlerClass: Type[http.server.BaseHTTPRequestHandler],
        max_concurrent_requests: int = 64,
    ):
        self._concurrency_sem = threading.BoundedSemaphore(max_concurrent_requests)
        super().__init__(server_address, RequestHandlerClass)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._concurrency_sem.acquire(blocking=False):
            try:
                body = b'{"error":"Server concurrency limit exceeded"}'
                resp = (
                    b"HTTP/1.1 503 Service Unavailable\r\n"
                    b"Content-Type: application/json\r\n"
                    + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii")
                    + body
                )
                request.sendall(resp)
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return

        t = threading.Thread(
            target=self.process_request_thread,
            args=(request, client_address),
            daemon=self.daemon_threads,
        )
        t.start()

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._concurrency_sem.release()


class JsonRequestHandler(http.server.BaseHTTPRequestHandler):
    service_name: str = "service"

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _send_bytes(self, status: int, content_type: str, body: bytes, cors: bool = False) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if cors:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-Id")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status: int, data: Dict[str, Any]) -> None:
        self._send_bytes(status, "application/json", json.dumps(data).encode("utf-8"), cors=True)

    def send_metrics(self, exposition: str) -> None:
        self._send_bytes(
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            exposition.encode("utf-8"),
        )

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-Id")
        self.end_headers()

    def read_json_body(self, max_bytes: int) -> Tuple[Optional[Dict[str, Any]], Optional[Tuple[int, Dict[str, Any]]]]:
        raw_len = self.headers.get("Content-Length", "0")
        try:
            content_length = int(raw_len)
        except ValueError:
            return None, (400, {"error": "Invalid Content-Length header", "service": self.service_name})
        if content_length < 0:
            return None, (400, {"error": "Invalid negative Content-Length", "service": self.service_name})
        if content_length > max_bytes:
            return None, (413, {"error": f"Payload exceeds {max_bytes} byte limit", "service": self.service_name})

        raw_body = self.rfile.read(content_length).decode("utf-8", errors="replace") if content_length > 0 else "{}"
        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as err:
            return None, (400, {"error": "Malformed JSON payload", "details": str(err), "service": self.service_name})

        if not isinstance(parsed, dict):
            return None, (400, {"error": "JSON body must be an object", "service": self.service_name})
        return parsed, None
