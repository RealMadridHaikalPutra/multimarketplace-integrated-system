"""
EXTERNAL SERVICE TASK — worker Camunda (service task type "External").

Worker ini MENGAMBIL tugas dari Camunda dan MENJALANKANNYA di backend ini:

    fetchAndLock  →  handler Python  →  complete / failure / bpmnError

Contoh pemakaian di BPMN (Camunda 7 Modeler):
  1. Bikin Service Task → Implementation: "External" → Topic: ``order-detail``
  2. Backend (worker ini) mengambil task itu, memanggil handler ``order_detail``
     (fetch detail order lewat APISIX), lalu mengirim variabel hasilnya balik →
     proses lanjut dengan variabel ``orderItems``, ``orderTotalQuantity``, dst.

Endpoint REST yang dipakai (dipetakan otomatis per versi engine):

  | Operasi        | Camunda 7 (engine-rest)                | Camunda 8 (v2, 8.6+)        |
  | -------------- | -------------------------------------- | --------------------------- |
  | fetch & lock   | POST /external-task/fetchAndLock       | POST /jobs/activation       |
  | complete       | POST /external-task/{id}/complete      | POST /jobs/{key}/completion |
  | failure        | POST /external-task/{id}/failure       | POST /jobs/{key}/failure    |
  | bpmn error     | POST /external-task/{id}/bpmnError     | POST /jobs/{key}/error      |
  | extend lock    | POST /external-task/{id}/extendLock    | (tidak didukung di C8)      |

Seperti modul camunda lain, konfigurasi ada di camunda/config.json bagian
``external_tasks`` (hot reload untuk endpoint manual / saat worker distart):

    "external_tasks": {
      "enabled": false,
      "worker_id": "sync-service-1",
      "max_tasks": 5,
      "poll_interval_seconds": 2,
      "topics": [
        {"topic": "order-detail", "handler": "order_detail",
         "variables": ["marketplace", "orderId", "shopId"]},
        {"topic": "send-signal", "handler": "send_signal"},
        {"topic": "stock-sync-part", "handler": "stock_sync_part"}
      ]
    }

Handler boleh berupa nama bawaan (lihat camunda/task_handlers.py) atau path
Python bebas: ``"handler": "aplikasi.custom:handle_task"``.

HANDLER
-------
``async def handler(task: ExternalTask) -> ExternalTaskResult | dict | None``

  - ``ExternalTaskResult`` → dipakai apa adanya (complete / failure / bpmnError).
  - ``dict``               → dianggap variabel hasil, task di-COMPLETE.
  - ``None``               → task di-COMPLETE tanpa variabel.
  - exception              → task dilaporkan FAILURE (retries diturunkan), dan
                             pesan exception masuk ``errorMessage``/``errorDetails``
                             sehingga terlihat di Cockpit + bisa di-retry engine.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from infrastructure.camunda.client import CamundaClient

logger = logging.getLogger(__name__)

# Kunci konfigurasi di camunda/config.json.
EXTERNAL_TASK_CONFIG_KEY = "external_tasks"

DEFAULT_WORKER_ID = "sync-service-1"
DEFAULT_MAX_TASKS = 5
DEFAULT_LOCK_DURATION_MS = 60_000
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
DEFAULT_IDLE_BACKOFF_SECONDS = 5.0
DEFAULT_RETRIES = 3
DEFAULT_RETRY_TIMEOUT_MS = 30_000

# Aksi yang bisa dikirim balik ke Camunda setelah handler selesai.
ACTION_COMPLETED = "completed"
ACTION_FAILED = "failed"
ACTION_BPMN_ERROR = "bpmn_error"
ACTION_NO_HANDLER = "no_handler"
ACTION_ERROR = "error"


# ---------------------------------------------------------------------------
# Konfigurasi topic
# ---------------------------------------------------------------------------


@dataclass
class ExternalTaskTopic:
    """Satu topic yang di-subscribe worker (dari config.json)."""

    name: str
    handler: str = ""  # nama bawaan task_handlers.HANDLERS atau "module:function"
    lock_duration_ms: int = DEFAULT_LOCK_DURATION_MS
    variables: List[str] = field(default_factory=list)  # variabel yang di-fetch
    local_variables: bool = False  # True = fetch juga variabel lokal
    deserialize_values: bool = True
    enabled: bool = True
    business_key: Optional[str] = None
    process_definition_id: Optional[str] = None
    process_definition_key: Optional[str] = None
    # ``batch: true`` → SEMUA task topic ini yang terambil dalam SATU poll
    # di-merge jadi SATU aksi handler (mis. satu POST /run-part untuk banyak
    # part_id). ``false`` (default) → dikerjakan SATU PER SATU per task.
    batch: bool = False

    @classmethod
    def from_dict(cls, raw: Any) -> Optional["ExternalTaskTopic"]:
        """Bangun topic dari dict config; None bila tidak valid/nonaktif."""
        if not isinstance(raw, dict):
            return None
        name = str(raw.get("topic") or raw.get("name") or "").strip()
        if not name or raw.get("enabled") is False:
            return None
        raw_variables = raw.get("variables")
        variables: List[str] = []
        if isinstance(raw_variables, (list, tuple)):
            variables = [str(item).strip() for item in raw_variables if str(item).strip()]
        try:
            lock_duration = int(raw.get("lock_duration_ms") or DEFAULT_LOCK_DURATION_MS)
        except (TypeError, ValueError):
            lock_duration = DEFAULT_LOCK_DURATION_MS
        return cls(
            name=name,
            handler=str(raw.get("handler") or "").strip(),
            lock_duration_ms=max(1000, lock_duration),
            variables=variables,
            local_variables=bool(raw.get("local_variables")),
            deserialize_values=raw.get("deserialize_values") is not False,
            enabled=True,
            batch=raw.get("batch") is True,
            business_key=str(raw.get("business_key") or "").strip() or None,
            process_definition_id=str(raw.get("process_definition_id") or "").strip() or None,
            process_definition_key=str(raw.get("process_definition_key") or "").strip() or None,
        )

    def fetch_body(self, worker_id: str) -> Dict[str, Any]:
        """Satu entri ``topics[]`` untuk body fetchAndLock (Camunda 7)."""
        body: Dict[str, Any] = {
            "topicName": self.name,
            "lockDuration": self.lock_duration_ms,
        }
        if self.variables:
            body["variables"] = list(self.variables)
        if self.local_variables:
            body["localVariables"] = True
        if self.deserialize_values:
            body["deserializeValues"] = True
        if self.business_key:
            body["businessKey"] = self.business_key
        if self.process_definition_id:
            body["processDefinitionId"] = self.process_definition_id
        elif self.process_definition_key:
            body["processDefinitionId"] = self.process_definition_key
        return body

    def to_dict(self) -> Dict[str, Any]:
        return {
            "topic": self.name,
            "handler": self.handler or None,
            "lock_duration_ms": self.lock_duration_ms,
            "variables": list(self.variables),
            "local_variables": self.local_variables,
            "deserialize_values": self.deserialize_values,
            "batch": self.batch,
            "business_key": self.business_key,
            "process_definition_id": self.process_definition_id,
            "process_definition_key": self.process_definition_key,
        }


# ---------------------------------------------------------------------------
# Task & hasil
# ---------------------------------------------------------------------------


@dataclass
class ExternalTask:
    """Satu external service task hasil fetchAndLock (sudah di-normalisasi)."""

    id: str
    topic_name: str
    worker_id: Optional[str] = None
    lock_expiration_time: Optional[str] = None
    process_instance_id: Optional[str] = None
    execution_id: Optional[str] = None
    activity_id: Optional[str] = None
    activity_instance_id: Optional[str] = None
    process_definition_id: Optional[str] = None
    process_definition_key: Optional[str] = None
    business_key: Optional[str] = None
    retries: Optional[int] = None
    error_message: Optional[str] = None
    tenant_id: Optional[str] = None
    priority: Optional[int] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    local_variables: Dict[str, Any] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _unwrap_variables(raw_vars: Any) -> Dict[str, Any]:
        """Nilai variabel fetchAndLock → tipe aslinya (buang bungkus typed).

        fetchAndLock Camunda 7 mengembalikan tiap variabel dalam bentuk
        ``{"type": "Long", "value": 3000100, "valueInfo": {}}``. Tanpa
        di-unwrap, ``task.variable("partId")`` mendapat DICT → handler gagal
        parse (gejala di produksi: BPMN error PART_ID_MISSING 0ms padahal
        variabel jelas ada di instance, lalu instance mati diam-diam karena
        error-nya tidak ditangkap BPMN).

        Payload polos (uji coba/manual, Camunda 8) dan nilai biasa berbentuk
        objek (bukan bungkus typed) DIPERTAHANKAN apa adanya — bungkus typed
        hanya dikenali dari kunci ``type``/``value``/``valueInfo`` saja.
        """
        if not isinstance(raw_vars, dict):
            return {}
        typed_keys = {"type", "value", "valueInfo"}
        unwrapped: Dict[str, Any] = {}
        for name, item in raw_vars.items():
            if (
                isinstance(item, dict)
                and "value" in item
                and set(item).issubset(typed_keys)
            ):
                unwrapped[name] = item.get("value")
            else:
                unwrapped[name] = item
        return unwrapped

    @classmethod
    def from_raw(cls, raw: Dict[str, Any]) -> "ExternalTask":
        """Normalisasi payload task (Camunda 7) / job (Camunda 8) → ExternalTask.

        Variabel TYPED hasil fetchAndLock (``{type, value, valueInfo}``)
        di-unwrap jadi nilai aslinya — lihat :meth:`_unwrap_variables`.
        """
        raw = raw if isinstance(raw, dict) else {}
        task_id = str(
            raw.get("id") or raw.get("jobKey") or raw.get("key") or ""
        ).strip()
        topic = str(raw.get("topicName") or raw.get("type") or "").strip()
        variables = raw.get("variables")
        local_variables = raw.get("localVariables")
        return cls(
            id=task_id,
            topic_name=topic,
            worker_id=raw.get("workerId") or raw.get("worker"),
            lock_expiration_time=raw.get("lockExpirationTime"),
            process_instance_id=raw.get("processInstanceId"),
            execution_id=raw.get("executionId"),
            activity_id=raw.get("activityId") or raw.get("elementId"),
            activity_instance_id=raw.get("activityInstanceId"),
            process_definition_id=raw.get("processDefinitionId"),
            process_definition_key=raw.get("processDefinitionKey"),
            business_key=raw.get("businessKey"),
            retries=raw.get("retries") if isinstance(raw.get("retries"), int) else None,
            error_message=raw.get("errorMessage"),
            tenant_id=raw.get("tenantId"),
            priority=raw.get("priority"),
            variables=cls._unwrap_variables(variables),
            local_variables=cls._unwrap_variables(local_variables),
            raw=raw,
        )

    def variable(self, *names: str, default: Any = None) -> Any:
        """Ambil variabel pertama yang ada (boleh beberapa nama alias)."""
        for name in names:
            if not str(name).strip():
                continue
            if name in self.variables and self.variables[name] not in (None, ""):
                return self.variables[name]
        return default

    def text(self, *names: str, default: str = "") -> str:
        """Seperti ``variable()`` tapi dipaksa jadi string (tanpa spasi tepi)."""
        value = self.variable(*names, default=None)
        if value is None:
            return default
        return str(value).strip()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic_name,
            "worker_id": self.worker_id,
            "process_instance_id": self.process_instance_id,
            "execution_id": self.execution_id,
            "process_definition_key": self.process_definition_key,
            "business_key": self.business_key,
            "retries": self.retries,
            "priority": self.priority,
            "variables": self.variables,
        }


@dataclass
class ExternalTaskResult:
    """Hasil eksekusi handler → aksi yang dikirim balik ke Camunda."""

    complete: bool = True
    variables: Dict[str, Any] = field(default_factory=dict)
    variable_types: Dict[str, str] = field(default_factory=dict)
    local_variables: Dict[str, Any] = field(default_factory=dict)
    # failure (retry menurut `retries`; 0 = incident di engine)
    failure: bool = False
    error_message: str = ""
    error_details: str = ""
    retries: Optional[int] = None
    retry_timeout_ms: Optional[int] = None
    # bpmn error (menuju boundary error event di BPMN)
    bpmn_error: bool = False
    error_code: str = ""

    @classmethod
    def ok(
        cls,
        variables: Optional[Dict[str, Any]] = None,
        variable_types: Optional[Dict[str, str]] = None,
        local_variables: Optional[Dict[str, Any]] = None,
    ) -> "ExternalTaskResult":
        """Task berhasil → COMPLETE + variabel hasil."""
        return cls(
            complete=True,
            variables=dict(variables or {}),
            variable_types=dict(variable_types or {}),
            local_variables=dict(local_variables or {}),
        )

    @classmethod
    def fail(
        cls,
        error_message: str,
        error_details: str = "",
        retries: Optional[int] = None,
        retry_timeout_ms: Optional[int] = None,
    ) -> "ExternalTaskResult":
        """Task gagal → FAILURE (engine akan me-retry sesuai `retries`)."""
        return cls(
            complete=False,
            failure=True,
            error_message=str(error_message or ""),
            error_details=str(error_details or ""),
            retries=retries,
            retry_timeout_ms=retry_timeout_ms,
        )

    @classmethod
    def bpmn_error_result(
        cls,
        error_code: str,
        error_message: str = "",
        variables: Optional[Dict[str, Any]] = None,
    ) -> "ExternalTaskResult":
        """Task gagal bisnis (bukan teknis) → BPMN ERROR (boundary event)."""
        return cls(
            complete=False,
            bpmn_error=True,
            error_code=str(error_code or ""),
            error_message=str(error_message or ""),
            variables=dict(variables or {}),
        )


@dataclass
class ExternalTaskOutcome:
    """Ringkasan eksekusi SATU task (untuk log & endpoint manual)."""

    task_id: str
    topic: str
    action: str
    ok: bool
    handler: Optional[str] = None
    error: Optional[str] = None
    variables: List[str] = field(default_factory=list)
    duration_ms: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "topic": self.topic,
            "action": self.action,
            "ok": self.ok,
            "handler": self.handler,
            "error": self.error,
            "variables": self.variables,
            "duration_ms": self.duration_ms,
        }


ExternalTaskHandler = Callable[[ExternalTask], Awaitable[Any]]


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class ExternalTaskWorker:
    """Worker external service task: fetchAndLock → handler → complete/failure."""

    def __init__(
        self,
        client: Optional[CamundaClient] = None,
        worker_id: str = DEFAULT_WORKER_ID,
        topics: Optional[List[ExternalTaskTopic]] = None,
        max_tasks: int = DEFAULT_MAX_TASKS,
        poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
        idle_backoff_seconds: float = DEFAULT_IDLE_BACKOFF_SECONDS,
        default_lock_duration_ms: int = DEFAULT_LOCK_DURATION_MS,
        default_retries: int = DEFAULT_RETRIES,
        retry_timeout_ms: int = DEFAULT_RETRY_TIMEOUT_MS,
        use_priority: bool = True,
        async_response_timeout_ms: int = 0,
        report_handler_errors: bool = True,
        decrement_retries: bool = True,
    ):
        self._client = client
        self.worker_id = str(worker_id or DEFAULT_WORKER_ID)
        self.topics: List[ExternalTaskTopic] = list(topics or [])
        self.max_tasks = max(1, int(max_tasks or DEFAULT_MAX_TASKS))
        self.poll_interval_seconds = max(0.0, float(poll_interval_seconds or 0.0))
        self.idle_backoff_seconds = max(0.0, float(idle_backoff_seconds or 0.0))
        self.default_lock_duration_ms = int(default_lock_duration_ms or DEFAULT_LOCK_DURATION_MS)
        self.default_retries = int(default_retries or DEFAULT_RETRIES)
        self.retry_timeout_ms = int(retry_timeout_ms or DEFAULT_RETRY_TIMEOUT_MS)
        self.use_priority = bool(use_priority)
        self.async_response_timeout_ms = int(async_response_timeout_ms or 0)
        self.report_handler_errors = bool(report_handler_errors)
        self.decrement_retries = bool(decrement_retries)

        self.handlers: Dict[str, ExternalTaskHandler] = {}

        # Statistik & status loop (dipakai endpoint /status).
        self.started_at: Optional[datetime] = None
        self.last_poll_at: Optional[datetime] = None
        self.last_outcomes: List[ExternalTaskOutcome] = []
        self.last_error: Optional[str] = None
        self.iterations = 0
        self.total_fetched = 0
        self.total_completed = 0
        self.total_failed = 0
        self.total_bpmn_errors = 0
        self.total_errors = 0

        self._task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None

    # ------------------------------------------------------------------
    # Konfigurasi & handler
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: Optional[Dict[str, Any]] = None,
        client: Optional[CamundaClient] = None,
        load_handlers: bool = True,
    ) -> "ExternalTaskWorker":
        """Bangun worker dari bagian ``external_tasks`` di camunda/config.json."""
        section = external_task_config(config)
        topics = parse_topics(section)
        worker = cls(
            client=client,
            worker_id=str(section.get("worker_id") or DEFAULT_WORKER_ID),
            topics=topics,
            max_tasks=int(section.get("max_tasks") or DEFAULT_MAX_TASKS),
            poll_interval_seconds=float(
                section.get("poll_interval_seconds") or DEFAULT_POLL_INTERVAL_SECONDS
            ),
            idle_backoff_seconds=float(
                section.get("idle_backoff_seconds") or DEFAULT_IDLE_BACKOFF_SECONDS
            ),
            default_lock_duration_ms=int(
                section.get("default_lock_duration_ms") or DEFAULT_LOCK_DURATION_MS
            ),
            default_retries=int(section.get("default_retries") or DEFAULT_RETRIES),
            retry_timeout_ms=int(section.get("retry_timeout_ms") or DEFAULT_RETRY_TIMEOUT_MS),
            use_priority=section.get("use_priority") is not False,
            async_response_timeout_ms=int(section.get("async_response_timeout_ms") or 0),
            report_handler_errors=section.get("report_handler_errors") is not False,
            decrement_retries=section.get("decrement_retries") is not False,
        )
        if load_handlers:
            worker.load_builtin_handlers()
        return worker

    def register(self, topic: str, handler: ExternalTaskHandler) -> None:
        """Daftarkan handler untuk satu topic (menimpa yang lama)."""
        self.handlers[str(topic).strip()] = handler

    def load_builtin_handlers(self) -> List[str]:
        """Resolve handler tiap topic dari task_handlers (nama bawaan / module:func).

        @return daftar topic yang handler-nya GAGAL di-resolve (tidak akan
                diproses; task-nya dilepas kembali ke engine sebagai failure).
        """
        from infrastructure.camunda.task_handlers import load_handler  # import lokal: hindari siklus

        unresolved: List[str] = []
        for topic in self.topics:
            if topic.name in self.handlers:
                continue
            name = topic.handler or topic.name
            handler = load_handler(name)
            if handler is None:
                unresolved.append(topic.name)
                logger.error(
                    "Camunda external task: handler '%s' untuk topic '%s' tidak "
                    "dikenal — topic ini tidak diproses",
                    name, topic.name,
                )
                continue
            self.handlers[topic.name] = handler
        return unresolved

    @property
    def enabled_topics(self) -> List[ExternalTaskTopic]:
        """Topic yang aktif DAN punya handler."""
        return [topic for topic in self.topics if topic.name in self.handlers]

    # ------------------------------------------------------------------
    # Client
    # ------------------------------------------------------------------

    def _get_client(self) -> CamundaClient:
        if self._client is None:
            self._client = CamundaClient()
        return self._client

    async def aclose(self) -> None:
        """Tutup HTTP client milik worker (best-effort)."""
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001
                logger.debug("Camunda external task: gagal menutup client", exc_info=True)

    # ------------------------------------------------------------------
    # Operasi REST (dipetakan per versi engine)
    # ------------------------------------------------------------------

    def _version(self) -> str:
        return "8" if str(getattr(self._get_client(), "version", "7")) == "8" else "7"

    async def fetch_and_lock(self, topics: Optional[List[ExternalTaskTopic]] = None) -> Tuple[List[ExternalTask], Optional[str]]:
        """Ambil + lock task dari Camunda.

        @return (tasks, error) — error diisi bila request gagal (tasks kosong).
        """
        client = self._get_client()
        topics = topics if topics is not None else self.enabled_topics
        if not topics:
            return [], None

        version = self._version()
        tasks: List[ExternalTask] = []

        if version == "8":
            # Camunda 8: aktivasi per tipe job (satu request per topic).
            for topic in topics:
                payload: Dict[str, Any] = {
                    "type": topic.name,
                    "maxJobsToActivate": self.max_tasks,
                    "worker": self.worker_id,
                    "timeout": topic.lock_duration_ms,
                    "requestTimeout": topic.lock_duration_ms,
                }
                if topic.variables:
                    payload["fetchVariable"] = list(topic.variables)
                if self.async_response_timeout_ms:
                    payload["requestTimeout"] = self.async_response_timeout_ms
                response, error, _ = await client.post_json(
                    client.rest_url("jobs", "activation"), payload
                )
                if error:
                    return tasks, error
                data = _json_body(response)
                raw_jobs = data.get("jobs") if isinstance(data, dict) else None
                for raw in raw_jobs or []:
                    if isinstance(raw, dict):
                        tasks.append(ExternalTask.from_raw(raw))
            return tasks, None

        payload = {
            "workerId": self.worker_id,
            "maxTasks": self.max_tasks,
            "usePriority": self.use_priority,
            "topics": [topic.fetch_body(self.worker_id) for topic in topics],
        }
        if self.async_response_timeout_ms:
            payload["asyncResponseTimeout"] = self.async_response_timeout_ms

        response, error, _ = await client.post_json(
            client.rest_url("external-task", "fetchAndLock"), payload
        )
        if error:
            return tasks, error

        data = _json_body(response)
        for raw in data if isinstance(data, list) else []:
            if isinstance(raw, dict):
                tasks.append(ExternalTask.from_raw(raw))
        return tasks, None

    async def complete(
        self,
        task: ExternalTask,
        variables: Optional[Dict[str, Any]] = None,
        variable_types: Optional[Dict[str, str]] = None,
        local_variables: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Selesaikan task + kirim variabel hasil. @return (ok, error)."""
        client = self._get_client()
        body: Dict[str, Any] = {
            "variables": client.build_variables(variables or {}, variable_types or {}),
        }
        if local_variables:
            body["localVariables"] = client.build_variables(local_variables, {})

        if self._version() == "8":
            url = client.rest_url("jobs", task.id, "completion")
            body.pop("localVariables", None)
        else:
            url = client.rest_url("external-task", task.id, "complete")
            body["workerId"] = self.worker_id

        response, error, _ = await client.post_json(url, body)
        return (error is None and response is not None), error

    async def handle_failure(
        self,
        task: ExternalTask,
        error_message: str,
        error_details: str = "",
        retries: Optional[int] = None,
        retry_timeout_ms: Optional[int] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Laporkan kegagalan teknis (engine me-retry sesuai `retries`)."""
        client = self._get_client()
        final_retries = self._failure_retries(task, retries)
        body: Dict[str, Any] = {
            "errorMessage": str(error_message or "handler failed"),
            "errorDetails": str(error_details or ""),
            "retries": final_retries,
        }
        timeout = retry_timeout_ms if retry_timeout_ms is not None else self.retry_timeout_ms
        if self._version() == "8":
            url = client.rest_url("jobs", task.id, "failure")
            if timeout:
                body["retryBackOff"] = int(timeout)
        else:
            url = client.rest_url("external-task", task.id, "failure")
            body["workerId"] = self.worker_id
            if timeout:
                body["retryTimeout"] = int(timeout)

        response, error, _ = await client.post_json(url, body)
        return (error is None and response is not None), error

    async def handle_bpmn_error(
        self,
        task: ExternalTask,
        error_code: str,
        error_message: str = "",
        variables: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Laporkan error BISNIS → boundary error event di BPMN."""
        client = self._get_client()
        body: Dict[str, Any] = {
            "errorCode": str(error_code or "TASK_ERROR"),
            "errorMessage": str(error_message or ""),
            "variables": client.build_variables(variables or {}, {}),
        }
        if self._version() == "8":
            url = client.rest_url("jobs", task.id, "error")
        else:
            url = client.rest_url("external-task", task.id, "bpmnError")
            body["workerId"] = self.worker_id

        response, error, _ = await client.post_json(url, body)
        return (error is None and response is not None), error

    async def extend_lock(
        self, task: ExternalTask, new_duration_ms: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """Perpanjang lock task (Camunda 7)."""
        client = self._get_client()
        if self._version() == "8":
            return False, "extendLock tidak didukung pada Camunda 8"
        duration = int(new_duration_ms or self.default_lock_duration_ms)
        response, error, _ = await client.post_json(
            client.rest_url("external-task", task.id, "extendLock"),
            {"workerId": self.worker_id, "newDuration": duration},
        )
        return (error is None and response is not None), error

    async def unlock(self, task: ExternalTask) -> Tuple[bool, Optional[str]]:
        """Lepaskan lock task supaya worker lain bisa mengambilnya."""
        client = self._get_client()
        if not task.id:
            return False, "task id kosong"
        response, error, _ = await client.post_json(
            client.rest_url("external-task", task.id, "unlock"),
            {"workerId": self.worker_id},
        )
        return (error is None and response is not None), error

    def _failure_retries(self, task: ExternalTask, retries: Optional[int]) -> int:
        """Jumlah retry yang dilaporkan saat failure."""
        if retries is not None:
            return max(0, int(retries))
        current = task.retries if isinstance(task.retries, int) else self.default_retries
        if not self.decrement_retries:
            return max(0, int(current))
        return max(0, int(current) - 1)

    # ------------------------------------------------------------------
    # Eksekusi
    # ------------------------------------------------------------------

    async def execute_task(
        self, task: ExternalTask, result: Any = None
    ) -> ExternalTaskOutcome:
        """Jalankan SATU task: panggil handler → kirim complete/failure/bpmnError.

        @param result: hasil handler (bila sudah dijalankan pemanggil — dipakai
                       test); None → handler dipanggil di sini.
        """
        handler = self.handlers.get(task.topic_name)
        started = time.perf_counter()

        if handler is None:
            # Tidak ada handler → jangan menahan task: laporkan failure supaya
            # engine me-retry (dan task kembali terlihat di Cockpit).
            ok, error = await self.handle_failure(
                task,
                f"no handler registered for topic '{task.topic_name}'",
                error_details=f"worker={self.worker_id}",
            )
            self.total_errors += 1
            return ExternalTaskOutcome(
                task_id=task.id,
                topic=task.topic_name,
                action=ACTION_NO_HANDLER,
                ok=ok,
                error=error or f"no handler for topic '{task.topic_name}'",
                duration_ms=int((time.perf_counter() - started) * 1000),
            )

        handler_name = getattr(handler, "__name__", str(handler))
        try:
            if result is None:
                result = await handler(task)
        except Exception as exc:  # noqa: BLE001 — handler tidak boleh mematikan worker
            logger.exception(
                "Camunda external task: handler '%s' gagal (task=%s topic=%s): %s",
                handler_name, task.id, task.topic_name, exc,
            )
            if not self.report_handler_errors:
                # Tanpa laporan: task dibiarkan sampai lock-nya habis (engine
                # akan menawarkannya lagi) — dihitung sebagai ERROR worker.
                self.total_errors += 1
                return ExternalTaskOutcome(
                    task_id=task.id,
                    topic=task.topic_name,
                    action=ACTION_ERROR,
                    ok=False,
                    handler=handler_name,
                    error=f"{type(exc).__name__}: {exc}",
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )
            ok, error = await self.handle_failure(
                task,
                f"{type(exc).__name__}: {exc}",
                error_details=_exception_details(exc),
            )
            self.total_failed += 1
            return ExternalTaskOutcome(
                task_id=task.id,
                topic=task.topic_name,
                action=ACTION_FAILED,
                ok=ok,
                handler=handler_name,
                error=error or f"{type(exc).__name__}: {exc}",
                duration_ms=int((time.perf_counter() - started) * 1000),
            )

        outcome = await self._apply_result(task, result, handler_name, started)
        return outcome

    async def _apply_result(
        self,
        task: ExternalTask,
        result: Any,
        handler_name: str,
        started: float,
    ) -> ExternalTaskOutcome:
        """Terjemahkan hasil handler → aksi Camunda + catat statistik."""
        duration_ms = int((time.perf_counter() - started) * 1000)

        if result is None:
            result = ExternalTaskResult.ok()
        elif isinstance(result, dict):
            result = ExternalTaskResult.ok(variables=result)
        elif not isinstance(result, ExternalTaskResult):
            # Tipe lain (str/int/objek) → dianggap satu variabel "result".
            result = ExternalTaskResult.ok(variables={"result": result})

        if result.bpmn_error:
            ok, error = await self.handle_bpmn_error(
                task, result.error_code, result.error_message, result.variables
            )
            self.total_bpmn_errors += 1
            # Selalu sertakan KODE error di outcome supaya log worker mencetak
            # "errorCode=PART_ID_MISSING …" — bukan hanya aksi "bpmn_error"
            # (debug produksi: tadi semua tampak sama tanpa kode sama sekali).
            detail = error or (
                f"errorCode={result.error_code}"
                + (f" — {result.error_message}" if result.error_message else "")
            )
            return ExternalTaskOutcome(
                task_id=task.id,
                topic=task.topic_name,
                action=ACTION_BPMN_ERROR,
                ok=ok,
                handler=handler_name,
                error=detail,
                variables=sorted(result.variables or {}),
                duration_ms=duration_ms,
            )

        if result.failure or not result.complete:
            if not result.error_message:
                result.error_message = "handler reported failure"
            ok, error = await self.handle_failure(
                task,
                result.error_message,
                result.error_details,
                retries=result.retries,
                retry_timeout_ms=result.retry_timeout_ms,
            )
            self.total_failed += 1
            return ExternalTaskOutcome(
                task_id=task.id,
                topic=task.topic_name,
                action=ACTION_FAILED,
                ok=ok,
                handler=handler_name,
                error=error or result.error_message,
                variables=sorted(result.variables or {}),
                duration_ms=duration_ms,
            )

        ok, error = await self.complete(
            task,
            variables=result.variables,
            variable_types=result.variable_types,
            local_variables=result.local_variables,
        )
        self.total_completed += 1
        return ExternalTaskOutcome(
            task_id=task.id,
            topic=task.topic_name,
            action=ACTION_COMPLETED,
            ok=ok,
            handler=handler_name,
            error=error,
            variables=sorted(result.variables or {}),
            duration_ms=duration_ms,
        )

    async def poll_once(self) -> List[ExternalTaskOutcome]:
        """Satu siklus: fetchAndLock → jalankan semua handler → lapor balik."""
        self.iterations += 1
        self.last_poll_at = datetime.now(timezone.utc)

        if not self.enabled_topics:
            self.last_error = "tidak ada topic dengan handler terdaftar"
            logger.warning("Camunda external task: %s", self.last_error)
            return []

        try:
            tasks, error = await self.fetch_and_lock()
        except Exception as exc:  # noqa: BLE001 — loop harus tetap hidup
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.total_errors += 1
            logger.exception("Camunda external task: fetchAndLock gagal: %s", exc)
            return []

        if error:
            self.last_error = error
            self.total_errors += 1
            logger.warning("Camunda external task: fetchAndLock gagal — %s", error)
            return []

        self.last_error = None
        self.total_fetched += len(tasks)

        service_tasks = [
            task for task in tasks if task.topic_name == "serviceWebhookTest"
        ]
        if service_tasks:
            from infrastructure.camunda.task_handlers import _extract_part_ids

            unique_part_ids: List[int] = []
            for task in service_tasks:
                for part_id in _extract_part_ids(
                    task.variable("partId", "part_id", "partID")
                ):
                    if part_id not in unique_part_ids:
                        unique_part_ids.append(part_id)
            logger.info(
                "Camunda serviceWebhook poll: jobs=%d unique_part_ids=%s",
                len(service_tasks),
                unique_part_ids,
            )
        else:
            logger.info(
                "Camunda external task poll: tidak ada serviceWebhookTest; "
                "polling berikutnya mengikuti idle_backoff=%.1fs",
                self.idle_backoff_seconds,
            )

        # Partisi: topik dengan `batch: true` (dan handler siap batch)
        # dikelompokkan → SATU aksi untuk semua tasknya; sisanya dikerjakan
        # satu per satu (perilaku lama, tetap default).
        normal_tasks: List[ExternalTask] = []
        batched: Dict[str, List[ExternalTask]] = {}
        for task in tasks:
            if self._is_batch_topic(task.topic_name):
                batched.setdefault(task.topic_name, []).append(task)
            else:
                normal_tasks.append(task)

        outcomes: List[ExternalTaskOutcome] = []
        for task in normal_tasks:
            outcome = await self.execute_task(task)
            outcomes.append(outcome)
            self._log_outcome(outcome)

        for topic_name, group in batched.items():
            for outcome in await self.execute_batch_group(topic_name, group):
                outcomes.append(outcome)
                self._log_outcome(outcome)

        self.last_outcomes = outcomes
        return outcomes

    # ------------------------------------------------------------------
    # Mode BATCH — topik `batch: true` di config.json
    # ------------------------------------------------------------------

    @staticmethod
    def _log_outcome(outcome: ExternalTaskOutcome) -> None:
        log = logger.info if outcome.ok else logger.error
        log(
            "Camunda external task %s: task=%s topic=%s handler=%s "
            "variabel=%s durasi=%dms%s",
            outcome.action,
            outcome.task_id,
            outcome.topic,
            outcome.handler,
            outcome.variables,
            outcome.duration_ms,
            f" error={outcome.error}" if outcome.error else "",
        )

    def _topic_config(self, topic_name: str) -> Optional[ExternalTaskTopic]:
        for topic in self.topics:
            if topic.name == topic_name:
                return topic
        return None

    def _is_batch_topic(self, topic_name: str) -> bool:
        """True bila topik diminta ``batch: true`` DAN handler mendukung batch.

        Handler menandai kesiapannya dengan atribut ``batch_capable = True``.
        Tanpa atribut itu topik otomatis dikerjakan SATU PER SATU (aman).
        """
        topic = self._topic_config(topic_name)
        if topic is None or not topic.batch:
            return False
        handler = self.handlers.get(topic_name)
        if handler is None:
            return False
        if not getattr(handler, "batch_capable", False):
            logger.warning(
                "Camunda external task: topik '%s' mengatur batch=true tetapi "
                "handler '%s' tidak batch_capable — dikerjakan SATU PER SATU.",
                topic_name,
                getattr(handler, "__name__", str(handler)),
            )
            return False
        return True

    @staticmethod
    def _merge_batch_tasks(tasks: List[ExternalTask]) -> ExternalTask:
        """Gabung banyak task (SATU topik) menjadi SATU task virtual.

        Semua nilai ``partId`` di-merge + dedup (satu product bisa memetakan ke
        banyak part, dan banyak task bisa membawa part berbeda) sehingga handler
        menerima SATU daftar part_id gabungan. Variabel lain memakai nilai task
        pertama — cukup untuk kebutuhan handler ``service_webhook_test``.
        """
        if len(tasks) == 1:
            return tasks[0]

        from infrastructure.camunda.task_handlers import _extract_part_ids  # lokal: hindari siklus

        first = tasks[0]
        merged_vars = dict(first.variables)
        merged_parts: List[int] = []
        part_keys: set = set()

        for task in tasks:
            raw = None
            for key in ("partId", "part_id", "partID"):
                if key in task.variables:
                    part_keys.add(key)
                    if raw is None:
                        raw = task.variables[key]
            for part_id in _extract_part_ids(raw):
                if part_id not in merged_parts:
                    merged_parts.append(part_id)

        merged_log_ids: List[Any] = []
        for task in tasks:
            raw_log_id = task.variable("webhookLogId", "webhook_log_id")
            if isinstance(raw_log_id, (list, tuple, set)):
                values = raw_log_id
            else:
                values = [raw_log_id]
            for log_id in values:
                if log_id not in (None, "") and log_id not in merged_log_ids:
                    merged_log_ids.append(log_id)

        for key in part_keys:
            merged_vars[key] = merged_parts
        if merged_log_ids:
            merged_vars["webhookLogId"] = merged_log_ids

        virtual = replace(
            first,
            variables=merged_vars,
            raw={**first.raw, "batch_tasks": [task.id for task in tasks]},
        )
        logger.info(
            "Camunda external task BATCH merge: %d task → partId=%s (virtual=%s)",
            len(tasks),
            merged_parts,
            virtual.id,
        )
        return virtual

    async def execute_batch_group(
        self, topic_name: str, tasks: List[ExternalTask]
    ) -> List[ExternalTaskOutcome]:
        """Jalankan handler SATU KALI untuk SEMUA task topik batch.

        Hasil handler diterapkan ke SETIAP task (complete / failure / bpmnError),
        jadi N task hanya memicu N laporan balik tapi SATU aksi bisnis
        (mis. satu POST ``/run-part`` berisi semua part_id).
        """
        if not tasks:
            return []

        handler = self.handlers.get(topic_name)
        if handler is None:
            # Sama dengan perilaku per-task: laporkan failure tiap task.
            return [await self.execute_task(task) for task in tasks]

        handler_name = getattr(handler, "__name__", str(handler))
        started = time.perf_counter()
        virtual = self._merge_batch_tasks(tasks)

        try:
            result = await handler(virtual)
        except Exception as exc:  # noqa: BLE001 — loop tidak boleh mati
            logger.exception(
                "Camunda external task BATCH handler '%s' gagal (topik=%s, %d task): %s",
                handler_name,
                topic_name,
                len(tasks),
                exc,
            )
            if not self.report_handler_errors:
                self.total_errors += len(tasks)
                return [
                    ExternalTaskOutcome(
                        task_id=task.id,
                        topic=topic_name,
                        action=ACTION_ERROR,
                        ok=False,
                        handler=handler_name,
                        error=f"{type(exc).__name__}: {exc}",
                        duration_ms=int((time.perf_counter() - started) * 1000),
                    )
                    for task in tasks
                ]
            result = ExternalTaskResult.fail(
                f"{type(exc).__name__}: {exc}",
                error_details=_exception_details(exc),
            )

        if result is None:
            result = ExternalTaskResult.ok()
        elif isinstance(result, dict):
            result = ExternalTaskResult.ok(variables=result)
        elif not isinstance(result, ExternalTaskResult):
            result = ExternalTaskResult.ok(variables={"result": result})

        return [
            await self._apply_result(task, result, handler_name, started)
            for task in tasks
        ]

    # ------------------------------------------------------------------
    # Loop background
    # ------------------------------------------------------------------

    async def run(
        self,
        stop_event: Optional[asyncio.Event] = None,
        max_iterations: Optional[int] = None,
    ) -> None:
        """Loop polling sampai ``stop_event`` di-set / ``max_iterations`` tercapai."""
        stop_event = stop_event or asyncio.Event()
        idle_backoff = self.idle_backoff_seconds or self.poll_interval_seconds
        iterations = 0

        logger.info(
            "Camunda external task worker '%s' start: %d topic (%s), max_tasks=%d, "
            "poll=%.1fs, idle_backoff=%.1fs",
            self.worker_id,
            len(self.enabled_topics),
            ", ".join(topic.name for topic in self.enabled_topics) or "-",
            self.max_tasks,
            self.poll_interval_seconds,
            idle_backoff,
        )

        while not stop_event.is_set():
            if max_iterations is not None and iterations >= max_iterations:
                break
            iterations += 1

            try:
                done = await self.poll_once()
            except Exception as exc:  # noqa: BLE001 — loop tidak boleh mati
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.total_errors += 1
                logger.exception("Camunda external task: siklus gagal: %s", exc)
                done = []

            delay = self.poll_interval_seconds if done else idle_backoff
            if delay > 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass

        logger.info(
            "Camunda external task worker '%s' stop (iterasi=%d, fetched=%d, "
            "completed=%d, failed=%d, bpmn_error=%d, errors=%d)",
            self.worker_id,
            self.iterations,
            self.total_fetched,
            self.total_completed,
            self.total_failed,
            self.total_bpmn_errors,
            self.total_errors,
        )

    def is_running(self) -> bool:
        """True bila loop background sedang hidup."""
        return self._task is not None and not self._task.done()

    async def start(self) -> bool:
        """Jalankan loop sebagai asyncio task. @return True bila baru distart."""
        if self.is_running():
            logger.info("Camunda external task worker '%s' sudah berjalan", self.worker_id)
            return False
        self.started_at = datetime.now(timezone.utc)
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self.run(self._stop_event))
        return True

    async def stop(self, timeout: float = 10.0) -> bool:
        """Hentikan loop background (tunggu selesai), lalu tutup client."""
        task, self._task = self._task, None
        if task is None:
            return False
        if self._stop_event is not None:
            self._stop_event.set()
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.TimeoutError:
            task.cancel()
            logger.warning(
                "Camunda external task worker '%s': stop timeout %.0fs — task dibatalkan",
                self.worker_id, timeout,
            )
        except Exception:  # noqa: BLE001
            logger.debug("Camunda external task: loop berhenti dengan error", exc_info=True)
        self._stop_event = None
        return True

    # ------------------------------------------------------------------
    # Ringkasan status (endpoint debug)
    # ------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """Status worker + statistik (aman untuk response JSON)."""
        return {
            "worker_id": self.worker_id,
            "running": self.is_running(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_poll_at": self.last_poll_at.isoformat() if self.last_poll_at else None,
            "last_error": self.last_error,
            "iterations": self.iterations,
            "topics": [topic.to_dict() for topic in self.topics],
            "handlers": sorted(self.handlers.keys()),
            "unhandled_topics": [
                topic.name for topic in self.topics if topic.name not in self.handlers
            ],
            "config": {
                "max_tasks": self.max_tasks,
                "poll_interval_seconds": self.poll_interval_seconds,
                "idle_backoff_seconds": self.idle_backoff_seconds,
                "default_lock_duration_ms": self.default_lock_duration_ms,
                "default_retries": self.default_retries,
                "retry_timeout_ms": self.retry_timeout_ms,
                "async_response_timeout_ms": self.async_response_timeout_ms,
                "use_priority": self.use_priority,
                "decrement_retries": self.decrement_retries,
                "report_handler_errors": self.report_handler_errors,
            },
            "totals": {
                "fetched": self.total_fetched,
                "completed": self.total_completed,
                "failed": self.total_failed,
                "bpmn_errors": self.total_bpmn_errors,
                "errors": self.total_errors,
            },
            "last_outcomes": [outcome.to_dict() for outcome in self.last_outcomes[-10:]],
        }


# ---------------------------------------------------------------------------
# Loader konfigurasi + singleton
# ---------------------------------------------------------------------------


def external_task_config(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Ambil bagian ``external_tasks`` dari camunda/config.json (hot reload)."""
    if config is None:
        from infrastructure.camunda.rules import load_rules  # import lokal: hindari siklus

        config = load_rules()
    section = (config or {}).get(EXTERNAL_TASK_CONFIG_KEY)
    return section if isinstance(section, dict) else {}


def parse_topics(section: Dict[str, Any]) -> List[ExternalTaskTopic]:
    """Daftar topic dari bagian konfigurasi (entri nonaktif/invalid dibuang)."""
    raw_topics = (section or {}).get("topics")
    topics: List[ExternalTaskTopic] = []
    for raw in raw_topics if isinstance(raw_topics, list) else []:
        topic = ExternalTaskTopic.from_dict(raw)
        if topic is not None:
            topics.append(topic)
    return topics


def external_tasks_enabled(section: Optional[Dict[str, Any]] = None) -> bool:
    """True bila worker harus auto-start (``external_tasks.enabled``)."""
    section = external_task_config() if section is None else section
    return bool((section or {}).get("enabled"))


_worker: Optional[ExternalTaskWorker] = None


def get_worker(reload_config: bool = False) -> ExternalTaskWorker:
    """Singleton worker (dikonfigurasi dari camunda/config.json).

    ``reload_config=True`` membangun ulang konfigurasi dari file — dipakai saat
    start otomatis (lifespan) dan endpoint manual supaya edit config.json
    langsung berlaku. Worker yang sedang berjalan TIDAK diganggu oleh reload
    ini kecuali loop-nya di-stop dulu (lihat ``stop_worker``).
    """
    global _worker
    if _worker is None or reload_config:
        if _worker is not None and _worker.is_running():
            logger.warning(
                "Camunda external task: reload konfigurasi diabaikan — worker "
                "sedang berjalan; stop dulu sebelum mengganti konfigurasi"
            )
            return _worker
        previous_client = _worker._client if _worker is not None else None
        _worker = ExternalTaskWorker.from_config(client=previous_client)
    return _worker


def set_worker(worker: Optional[ExternalTaskWorker]) -> None:
    """Ganti singleton (dipakai test / integrasi)."""
    global _worker
    _worker = worker


async def start_worker() -> Optional[ExternalTaskWorker]:
    """Start worker singleton bila ``external_tasks.enabled=true``.

    @return worker yang dijalankan, atau None bila nonaktif.
    """
    worker = get_worker(reload_config=True)
    if not external_tasks_enabled():
        logger.info(
            "Camunda external task worker DISABLED (external_tasks.enabled=false) — "
            "endpoint manual /api/v1/webhook/camunda/external-tasks/* tetap bisa "
            "dipakai untuk uji coba"
        )
        return None
    await worker.start()
    return worker


async def stop_worker() -> None:
    """Stop worker singleton + tutup HTTP client-nya (best-effort)."""
    global _worker
    worker, _worker = _worker, None
    if worker is None:
        return
    try:
        await worker.stop()
    except Exception:  # noqa: BLE001
        logger.debug("Camunda external task: gagal stop worker", exc_info=True)
    await worker.aclose()


# ---------------------------------------------------------------------------
# Helper murni
# ---------------------------------------------------------------------------


def _json_body(response: Any) -> Any:
    """Body JSON response httpx (None bila kosong / bukan JSON)."""
    if response is None:
        return None
    try:
        return response.json()
    except ValueError:
        text = (getattr(response, "text", "") or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except ValueError:
            return None


def _exception_details(exc: Exception, limit: int = 2000) -> str:
    """Detail exception (traceback ringkas) untuk kolom errorDetails Camunda."""
    import traceback

    text = "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__)
    ).strip()
    if len(text) > limit:
        text = text[:limit] + "...[truncated]"
    return text
