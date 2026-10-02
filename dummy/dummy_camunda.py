"""Dummy Camunda engine (engine-rest) — pengganti Camunda saat engine asli mati.

Meniru endpoint yang dipakai backend untuk alur webhook + external service task:

    POST /engine-rest/process-definition/key/{key}/start   → catat start event
    POST /engine-rest/message                              → catat message
    POST /engine-rest/signal                               → catat signal
    GET  /engine-rest/execution                            → [] (lookup subscriber)
    POST /engine-rest/external-task/fetchAndLock           → tawarkan task antri
    POST /engine-rest/external-task/{id}/complete          → catat hasil
    POST /engine-rest/external-task/{id}/failure           → catat hasil
    POST /engine-rest/external-task/{id}/bpmnError         → catat hasil
    GET  /__state                                          → dump state (JSON)

Perilaku kunci: saat menerima **start event**, variabelnya disimpan; task untuk
topic yang dikonfigurasi OTOMATIS diantrikan membawa variabel start tersebut
(mis. ``partId``) sehingga ``fetchAndLock`` terasa seperti engine sungguhan.

Jalankan mandiri:
    python -m dummy.dummy_camunda --port 8080
"""

from __future__ import annotations

import argparse
import json
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

DEFAULT_TOPICS = ["serviceWebhookTest"]


class DummyCamunda:
    """Engine Camunda palsu (stdlib) yang bisa dijalankan dalam thread."""

    def __init__(self, *, topics: Optional[List[str]] = None, port: int = 0) -> None:
        self.topics = list(topics or DEFAULT_TOPICS)
        self.starts: List[Dict[str, Any]] = []
        self.signals: List[Dict[str, Any]] = []
        self.messages: List[Dict[str, Any]] = []
        self.completions: List[Dict[str, Any]] = []
        self.failures: List[Dict[str, Any]] = []
        self.bpmn_errors: List[Dict[str, Any]] = []
        self.task_requests: List[Dict[str, Any]] = []

        self._queued: List[Dict[str, Any]] = []
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._requested_port = port

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> str:
        handler = self._build_handler()
        self._server = ThreadingHTTPServer(("127.0.0.1", self._requested_port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address[:2]
        self.base_url = f"http://{host}:{port}"
        return self.base_url

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unwrap_typed(variables: Any) -> Dict[str, Any]:
        """{name: {value,type}} → {name: {value,type}} (dibiarkan typed)."""
        if not isinstance(variables, dict):
            return {}
        out: Dict[str, Any] = {}
        for name, spec in variables.items():
            if isinstance(spec, dict) and "value" in spec:
                out[name] = {"value": spec.get("value"), "type": spec.get("type", "String")}
            else:
                out[name] = {"value": spec, "type": "String"}
        return out

    def _queue_tasks_for_start(self, body: Dict[str, Any]) -> None:
        variables = self._unwrap_typed(body.get("variables"))
        for topic in self.topics:
            self._queued.append(
                {
                    "id": f"task-{uuid.uuid4()}",
                    "topic": topic,
                    "variables": variables,
                    "processInstanceId": f"proc-{uuid.uuid4()}",
                    "businessKey": body.get("businessKey"),
                }
            )

    def last_start_variables(self) -> Dict[str, Any]:
        """Variabel (typed) dari start event terakhir; {} bila belum ada."""
        if not self.starts:
            return {}
        return self.starts[-1].get("variables") or {}

    def state(self) -> Dict[str, Any]:
        return {
            "starts": self.starts,
            "signals": self.signals,
            "messages": self.messages,
            "completions": self.completions,
            "failures": self.failures,
            "bpmn_errors": self.bpmn_errors,
            "queued": self._queued,
            "task_requests": self.task_requests,
        }

    # ------------------------------------------------------------------
    # HTTP handler
    # ------------------------------------------------------------------

    def _build_handler(self):
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # noqa: D401
                return

            # -- utilities ------------------------------------------------
            def _read_json(self) -> Dict[str, Any]:
                length = int(self.headers.get("Content-Length") or 0)
                if not length:
                    return {}
                raw = self.rfile.read(length)
                try:
                    data = json.loads(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    return {}
                return data if isinstance(data, dict) else {}

            def _send_json(self, payload: Any, status: int = 200) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _send_empty(self, status: int = 204) -> None:
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            # -- GET ------------------------------------------------------
            def do_GET(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                if path == "/__state":
                    self._send_json(outer.state())
                    return
                if path == "/health":
                    self._send_json({"status": "ok"})
                    return
                if path.endswith("/execution"):
                    self._send_json([])
                    return
                self._send_json({"error": "not found", "path": path}, status=404)

            # -- POST -----------------------------------------------------
            def do_POST(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                body = self._read_json()

                # Start / message / signal
                if path.endswith("/start") and "/process-definition/" in path:
                    key = path.split("/process-definition/", 1)[1].split("/", 1)[0]
                    if "/key/" in path:
                        key = path.split("/key/", 1)[1].split("/", 1)[0]
                    record = {
                        "key": key,
                        "request_type": "start",
                        "variables": outer._unwrap_typed(body.get("variables")),
                        "businessKey": body.get("businessKey"),
                        "tenantId": body.get("tenantId"),
                        "received_at": datetime.now(timezone.utc).isoformat(),
                    }
                    outer.starts.append(record)
                    outer._queue_tasks_for_start(body)
                    self._send_json(
                        {
                            "id": f"proc-{uuid.uuid4()}",
                            "definitionId": f"{key}:1:00000000-0000-0000-0000-000000000000",
                            "businessKey": body.get("businessKey"),
                            "ended": False,
                        }
                    )
                    return

                if path.endswith("/message"):
                    outer.messages.append(body)
                    self._send_json({"resultType": "Success", "executions": []})
                    return

                if path.endswith("/signal"):
                    outer.signals.append(body)
                    self._send_empty(204)
                    return

                # External task
                if path.endswith("/external-task/fetchAndLock"):
                    outer.task_requests.append(body)
                    topics = body.get("topics") if isinstance(body.get("topics"), list) else []
                    requested = {str(t.get("topicName")) for t in topics if isinstance(t, dict)}
                    tasks: List[Dict[str, Any]] = []
                    remaining: List[Dict[str, Any]] = []
                    for task in outer._queued:
                        if task["topic"] in requested and len(tasks) < len(requested):
                            tasks.append(
                                {
                                    "id": task["id"],
                                    "topicName": task["topic"],
                                    "workerId": body.get("workerId"),
                                    "processInstanceId": task["processInstanceId"],
                                    "executionId": task["processInstanceId"],
                                    "businessKey": task.get("businessKey"),
                                    "retries": 3,
                                    "variables": task["variables"],
                                }
                            )
                        else:
                            remaining.append(task)
                    outer._queued = remaining
                    self._send_json(tasks)
                    return

                if "/external-task/" in path:
                    task_id = path.split("/external-task/", 1)[1].split("/", 1)[0]
                    action = path.rsplit("/", 1)[-1]
                    entry = {"task_id": task_id, "action": action, "body": body}
                    if action == "complete":
                        outer.completions.append(entry)
                    elif action == "failure":
                        outer.failures.append(entry)
                    elif action in ("bpmnError", "error"):
                        outer.bpmn_errors.append(entry)
                    self._send_empty(204)
                    return

                self._send_json({"error": "not found", "path": path}, status=404)

        return _Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy Camunda engine-rest")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--topics", default="serviceWebhookTest")
    args = parser.parse_args()

    topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    server = DummyCamunda(topics=topics, port=args.port)
    base = server.start()
    print(f"Dummy Camunda berjalan di {base}/engine-rest (Ctrl+C untuk stop)")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
