"""
HTTP client Camunda — dipakai untuk START PROCESS / kirim MESSAGE dari webhook.

Mendukung dua versi engine, dipilih lewat env CAMUNDA_VERSION:

  Camunda 7 (default, rest path \"/engine-rest\")
      start by key : POST {base}/engine-rest/process-definition/key/{key}/start
      start by id  : POST {base}/engine-rest/process-definition/{id}/start
      message      : POST {base}/engine-rest/message
      Body variabel bertipe: {"variables": {"nama": {"value": ..., "type": "String"}}}

  Camunda 8 / Zeebe REST (rest path \"/v2\")
      start        : POST {base}/v2/process-instances
      Body variabel polos  : {"processDefinitionId": ..., "variables": {...}}

Semua nilai (base_url, version, rest path, auth, timeout, retry, ssl) diambil
dari `config.settings` sehingga bisa diubah lewat .env tanpa mengubah kode.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

from infrastructure.camunda.models import CamundaTriggerResult
from infrastructure.camunda.variables import (
    NUMERIC_VARIABLE_TYPES,
    build_typed_variable,
    coerce_variable_value,
    infer_variable_type,
    is_numeric_type,
    normalize_variable_type,
)
from core.config import settings

logger = logging.getLogger(__name__)

VERSION_ALIASES = {"7": "7", "7.x": "7", "c7": "7", "platform": "7",
                   "8": "8", "8.x": "8", "c8": "8", "zeebe": "8", "cloud": "8"}

# Alias `type` di config.json → tipe request internal. Dipakai supaya penamaan
# gaya helper JS ("start") maupun gaya REST ("start_process") sama-sama jalan.
REQUEST_TYPE_ALIASES = {
    "start": "start_process",
    "start_process": "start_process",
    "startprocess": "start_process",
    "start_process_instance": "start_process",
    "start_by_id": "start_process_by_id",
    "start_process_by_id": "start_process_by_id",
    "message": "message",
    "message_event": "message",
    "correlate_message": "message",
    "signal": "signal",
    "broadcast_signal": "signal",
    "signal_broadcast": "signal",
    "send_signal": "signal",
}


class CamundaConfigError(ValueError):    """Konfigurasi Camunda tidak lengkap/valid (gagal sebelum HTTP request)."""


def _parse_status_set(raw: Any) -> set:
    """\"429,500\" → {429, 500} (status HTTP yang memicu retry)."""
    result = set()
    for part in str(raw or "").split(","):
        part = part.strip()
        if part.isdigit():
            result.add(int(part))
    return result


def normalize_version(value: Any) -> str:
    """Normalisasi versi engine → \"7\" atau \"8\" (default \"7\")."""
    return VERSION_ALIASES.get(str(value or "7").strip().lower(), "7")


def normalize_request_type(value: Any) -> Optional[str]:
    """Normalisasi `type` aturan → start_process | start_process_by_id | message.

    @return nama kanonik, atau None bila tidak dikenal (pemanggil memberi warning).
    """
    key = str(value or "").strip().lower().replace("-", "_")
    return REQUEST_TYPE_ALIASES.get(key)



@dataclass(frozen=True)
class SignalSubscriptionMatch:
    """Hasil korelasi signal → execution (Camunda 7).

    ``found``           : ada process instance ORDER yang sedang menunggu signal ini.
    ``execution_id``    : execution yang LAYAK di-POST ``/execution/{id}/signal``
                          (punya ``activityId``). None bila tidak bisa di-target.
    ``subscriptions``   : jumlah execution yang menunggu (setelah filter order).
    """

    found: bool = False
    execution_id: Optional[str] = None
    subscriptions: int = 0


class CamundaClient:
    """Client HTTP Camunda (Camunda 7 engine-rest & Camunda 8 REST)."""

    def __init__(self, transport: Optional[httpx.AsyncBaseTransport] = None):
        self.base_url = str(settings.camunda_base_url or "").rstrip("/")
        self.version = normalize_version(settings.camunda_version)
        configured_rest = str(settings.camunda_rest_path or "").strip()
        if configured_rest:
            self.rest_path = "/" + configured_rest.strip("/")
        else:
            self.rest_path = "/engine-rest" if self.version == "7" else "/v2"

        self.timeout = float(settings.camunda_timeout_seconds or 10.0)
        self.max_attempts = max(1, int(settings.camunda_max_attempts or 1))
        self.retry_delay = float(settings.camunda_retry_delay_seconds or 0.0)
        self.retry_statuses = _parse_status_set(settings.camunda_retry_on_status)
        self.typed_variables = bool(settings.camunda_typed_variables)
        self.tenant_id = str(settings.camunda_tenant_id or "").strip() or None
        self.verify_ssl = bool(settings.camunda_verify_ssl)

        # Transport boleh di-inject (unit test memakai httpx.MockTransport).
        self._transport = transport
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # URL builders
    # ------------------------------------------------------------------

    def _root_url(self) -> str:
        """base_url + rest path, tanpa dobel bila base_url sudah memuatnya.

        Library external task client memakai default
        "http://localhost:8080/engine-rest" (sudah termasuk rest path) — kalau
        kita menempelkan "/engine-rest" lagi, URL-nya jadi
        "/engine-rest/engine-rest/...". Karena itu penempelan dilewati bila
        base_url sudah berakhiran rest path.
        """
        if self.base_url.lower().endswith(self.rest_path.lower()):
            return self.base_url
        return f"{self.base_url}{self.rest_path}"

    def build_url(
        self,
        request_type: str,
        key: Optional[str] = None,
        execution_id: Optional[str] = None,
    ) -> str:
        """Bangun URL endpoint Camunda sesuai versi & tipe request.

        Tipe yang didukung: ``start_process`` | ``start_process_by_id`` |
        ``message`` | ``signal``.
        """
        if not self.base_url:
            raise CamundaConfigError(
                "CAMUNDA_BASE_URL belum diisi — set mis. http://localhost:8080 "
                "(camunda_rest_path otomatis ditempel) atau http://localhost:8080/engine-rest"
            )

        root = self._root_url()

        if self.version == "8":
            # Camunda 8 REST (Orchestration Cluster API):
            #   start_process → /process-instances
            #   signal        → /signals/broadcast (tersedia sejak Camunda 8.6)
            # Signal per-eksekusi TIDAK ada di Camunda 8.
            if request_type == "signal":
                if execution_id:
                    raise CamundaConfigError(
                        "Camunda 8 tidak mendukung signal per-execution "
                        "(execution_id) — pakai broadcast signal"
                    )
                return f"{root}/signals/broadcast"
            return f"{root}/process-instances"

        if request_type == "signal":
            # Camunda 7: POST /signal = BROADCAST ke semua handler sinyal aktif.
            # Bila execution_id diisi → trigger satu eksekusi (/execution/{id}/signal).
            if execution_id:
                return f"{root}/execution/{execution_id}/signal"
            return f"{root}/signal"

        if request_type == "message":
            return f"{root}/message"
        if request_type == "start_process_by_id":
            if not key:
                raise CamundaConfigError("process_definition_id wajib untuk start_process_by_id")
            return f"{root}/process-definition/{key}/start"
        if not key:
            raise CamundaConfigError("process_definition_key wajib untuk start_process")
        return f"{root}/process-definition/key/{key}/start"

    # ------------------------------------------------------------------
    # Body builders
    # ------------------------------------------------------------------

    def build_variables(self, variables: Dict[str, Any], declared_types: Dict[str, str]) -> Dict[str, Any]:
        """Bangun blok variables (bertipe untuk v7, polos untuk v8)."""
        if self.version == "8" or not self.typed_variables:
            return dict(variables or {})

        result: Dict[str, Any] = {}
        for name, value in (variables or {}).items():
            # Nama tipe boleh lowercase di config.json ("string") — dinormalisasi
            # ke bentuk kanonik Camunda, sekaligus menyelaraskan nilainya.
            final_value, final_type = build_typed_variable(value, (declared_types or {}).get(name))
            result[name] = {"value": final_value, "type": final_type}
        return result

    def build_body(
        self,
        request_type: str,
        variables: Dict[str, Any],
        variable_types: Optional[Dict[str, str]] = None,
        business_key: Optional[str] = None,
        process_definition_id: Optional[str] = None,
        process_definition_key: Optional[str] = None,
        message_name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        signal_name: Optional[str] = None,
        execution_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Bangun body request start-process / message / signal."""
        body_variables = self.build_variables(variables, variable_types or {})
        tenant = tenant_id or self.tenant_id

        if self.version == "8":
            # Camunda 8 REST memakai processDefinitionId (key boleh dipakai
            # sebagai latest-version shortcut, sama seperti APISIX side).
            if request_type == "signal":
                # Broadcast signal C8: {"signalName": ..., "variables": {...}}.
                # Tidak ada businessKey di endpoint ini.
                body = {"signalName": signal_name or "", "variables": body_variables}
            else:
                body = {
                    "processDefinitionId": process_definition_id or process_definition_key or "",
                    "variables": body_variables,
                }
                if business_key:
                    body["businessKey"] = business_key
        elif request_type == "signal":
            # Camunda 7: broadcast memakai field "name"; trigger satu eksekusi
            # (/execution/{id}/signal) memakai field "signalName".
            # CATATAN: REST Camunda 7 TIDAK punya field businessKey untuk signal —
            # business key harus dikirim sebagai VARIABEL (lihat service.send_signal).
            if execution_id:
                body = {"signalName": signal_name or "", "variables": body_variables}
            else:
                body = {"name": signal_name or "", "variables": body_variables}
        elif request_type == "message":
            body = {
                "messageName": message_name or "",
                "processVariables": body_variables,
            }
            if business_key:
                body["businessKey"] = business_key
        else:
            body = {"variables": body_variables}
            if business_key:
                body["businessKey"] = business_key

        if tenant:
            body["tenantId"] = tenant

        for key, value in (extra_body or {}).items():
            body[key] = value
        return body

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "SynchronizationService-CamundaClient/1.0",
        }
        auth_type = str(settings.camunda_auth_type or "none").strip().lower()
        if auth_type == "bearer" and settings.camunda_token:
            headers["Authorization"] = f"Bearer {settings.camunda_token}"
        return headers

    def _auth(self) -> Optional[Tuple[str, str]]:
        """Basic auth httpx (None bila tidak dipakai)."""
        auth_type = str(settings.camunda_auth_type or "none").strip().lower()
        if auth_type == "basic":
            return (settings.camunda_username or "", settings.camunda_password or "")
        return None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                verify=self.verify_ssl,
                transport=self._transport,
            )
        return self._client

    async def aclose(self) -> None:
        """Tutup shared AsyncClient."""
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    async def _post_with_retry(
        self,
        url: str,
        body: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
        retry: bool = True,
    ) -> Tuple[Optional[httpx.Response], Optional[str], int]:
        """POST + retry terbatas. @return (response, error, jumlah percobaan).

        ``retry=False`` dipakai polling external task: loop pemanggil yang
        mengatur jeda, retry berlapis hanya menambah latensi.
        """
        client = self._get_client()
        last_error: Optional[str] = None
        max_attempts = self.max_attempts if retry else 1

        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.post(
                    url,
                    json=body,
                    headers=self._headers(),
                    auth=self._auth(),
                    params=params,
                )
            except httpx.RequestError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("Camunda: request error (percobaan %d/%d): %s", attempt, max_attempts, last_error)
            else:
                if 200 <= response.status_code < 300:
                    return response, None, attempt
                last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                logger.warning(
                    "Camunda: HTTP %s (percobaan %d/%d): %s",
                    response.status_code, attempt, max_attempts, response.text[:500],
                )
                if response.status_code not in self.retry_statuses:
                    return response, last_error, attempt

            if attempt < max_attempts and self.retry_delay > 0:
                await asyncio.sleep(self.retry_delay)

        return None, last_error or "unknown error", max_attempts

    # ------------------------------------------------------------------
    # Helper REST generik (dipakai operasi EXTERNAL SERVICE TASK)
    # ------------------------------------------------------------------

    def rest_url(self, *segments: Any) -> str:
        """URL absolut ke endpoint Camunda lain (root + segmen, tanpa dobel)."""
        root = self._root_url()
        parts = [str(segment).strip("/") for segment in segments if str(segment).strip("/")]
        return f"{root}/{'/'.join(parts)}" if parts else root

    async def post_json(
        self,
        url: str,
        body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        retry: bool = False,
    ) -> Tuple[Optional[httpx.Response], Optional[str], int]:
        """POST generik ke Camunda (fetchAndLock / complete / failure / bpmnError).

        Mengembalikan (response, error, attempts). Response 204 (No Content)
        dianggap sukses; body boleh kosong.
        """
        return await self._post_with_retry(url, body or {}, params=params, retry=retry)

    async def _get_with_retry(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        retry: bool = True,
    ) -> Tuple[Optional[httpx.Response], Optional[str], int]:
        """GET + retry terbatas (kueri execution / process instance).

        @return (response, error, jumlah percobaan) — sama seperti
                :meth:`_post_with_retry`. Response 204 (No Content) dianggap sukses.
        """
        client = self._get_client()
        last_error: Optional[str] = None
        max_attempts = self.max_attempts if retry else 1

        for attempt in range(1, max_attempts + 1):
            try:
                response = await client.get(
                    url,
                    headers=self._headers(),
                    auth=self._auth(),
                    params=params,
                )
            except httpx.RequestError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "Camunda: GET request error (percobaan %d/%d): %s",
                    attempt, max_attempts, last_error,
                )
            else:
                if 200 <= response.status_code < 300:
                    return response, None, attempt
                last_error = f"HTTP {response.status_code}: {response.text[:500]}"
                logger.warning(
                    "Camunda: GET HTTP %s (percobaan %d/%d): %s",
                    response.status_code, attempt, max_attempts, response.text[:500],
                )
                if response.status_code not in self.retry_statuses:
                    return response, last_error, attempt

            if attempt < max_attempts and self.retry_delay > 0:
                await asyncio.sleep(self.retry_delay)

        return None, last_error or "unknown error", max_attempts

    async def _get_json_list(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """GET → list dict (None bila gagal / bukan JSON array)."""
        response, error, _ = await self._get_with_retry(url, params=params)
        if response is None or error:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        return data if isinstance(data, list) else None

    async def match_signal_subscription(
        self,
        signal_name: str,
        business_key_like: Optional[str] = None,
        max_results: int = 200,
    ) -> SignalSubscriptionMatch:
        """Cek apakah ada execution yang MENUNGGU ``signal_name`` (Camunda 7).

        KORELASI (inti kenapa ini ada): signal Camunda itu BROADCAST — kalau
        instance order tidak sedang menunggu, signal hilang TANPA error (HTTP
        204). Bila ``business_key_like`` diisi (mis. ``"<invoice>:%"``), hanya
        process instance dengan business key cocok LIKE yang dihitung.

        Penting (dari error produksi
        ``cannot signal execution ... it has no current activity``): execution
        yang memegang subscription BOUNDARY event sering tidak punya
        ``activityId`` (menempel di scope execution) sehingga engine MENOLAK
        ``POST /execution/{id}/signal``. Karena itu ``execution_id`` hanya diisi
        bila execution itu memang layak di-signal; selainnya pemanggil wajib
        fallback ke broadcast (dengan nama signal yang sudah diperiksa ini).
        """
        if self.version != "7":
            return SignalSubscriptionMatch()
        name = str(signal_name or "").strip()
        if not name:
            return SignalSubscriptionMatch()

        allowed_pi: Optional[set] = None
        if business_key_like:
            items = await self._get_json_list(
                self.rest_url("process-instance"),
                {
                    "businessKeyLike": business_key_like,
                    "active": "true",
                    "maxResults": max_results,
                },
            )
            if items is None:
                return SignalSubscriptionMatch()
            allowed_pi = {str(it.get("id")) for it in items if it.get("id")}
            if not allowed_pi:
                return SignalSubscriptionMatch()

        executions = await self._get_json_list(
            self.rest_url("execution"),
            {"signalEventSubscriptionName": name, "maxResults": max_results},
        )
        if executions is None:
            return SignalSubscriptionMatch()

        matched: List[Dict[str, Any]] = []
        for item in executions:
            if not str(item.get("id") or "").strip():
                continue
            if allowed_pi is not None and str(item.get("processInstanceId")) not in allowed_pi:
                continue
            matched.append(item)

        if not matched:
            return SignalSubscriptionMatch(found=False, subscriptions=0)

        signalable = [
            str(item["id"]).strip()
            for item in matched
            if str(item.get("activityId") or "").strip()
        ]
        chosen = signalable[0] if signalable else None

        if chosen is None:
            logger.info(
                "Camunda: %d execution menunggu signal '%s' (business key LIKE %s), "
                "tapi tidak ada yang punya activityId — umum utk BOUNDARY event "
                "(subscription menempel di scope execution, CAMUNDA MENOLAK "
                "/execution/{id}/signal) → pengiriman memakai broadcast.",
                len(matched), name, business_key_like or "-",
            )
        elif len(signalable) > 1:
            logger.warning(
                "Camunda: %d execution layak di-signal untuk signal '%s' — "
                "memakai yang pertama (%s)",
                len(signalable), name, chosen,
            )

        return SignalSubscriptionMatch(
            found=True, execution_id=chosen, subscriptions=len(matched)
        )

    async def trigger(
        self,
        request_type: str,
        variables: Dict[str, Any],
        variable_types: Optional[Dict[str, str]] = None,
        process_definition_key: Optional[str] = None,
        process_definition_id: Optional[str] = None,
        message_name: Optional[str] = None,
        business_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        rule_id: Optional[str] = None,
        marketplace: Optional[str] = None,
        signal_code: Any = None,
        signal_name: Optional[str] = None,
        execution_id: Optional[str] = None,
    ) -> CamundaTriggerResult:
        """Kirim start-process / message / SIGNAL ke Camunda (retry + timing)."""
        key = process_definition_id if request_type == "start_process_by_id" else process_definition_key
        try:
            if request_type == "signal" and not str(signal_name or "").strip():
                raise CamundaConfigError(
                    "signal_name wajib diisi untuk request_type=signal"
                )
            url = self.build_url(request_type, key, execution_id=execution_id)
            body = self.build_body(
                request_type=request_type,
                variables=variables,
                variable_types=variable_types,
                business_key=business_key,
                process_definition_id=process_definition_id,
                process_definition_key=process_definition_key,
                message_name=message_name,
                tenant_id=tenant_id,
                extra_body=extra_body,
                signal_name=signal_name,
                execution_id=execution_id,
            )
        except CamundaConfigError as exc:
            logger.error("Camunda: konfigurasi tidak valid — %s", exc)
            return CamundaTriggerResult(
                ok=False,
                request_type=request_type,
                error=str(exc),
                variables=variables,
                rule_id=rule_id,
                marketplace=marketplace,
                signal_code=signal_code,
                signal_name=signal_name,
                execution_id=execution_id,
            )

        started = time.perf_counter()
        response, error, attempts = await self._post_with_retry(url, body)
        duration_ms = int((time.perf_counter() - started) * 1000)

        parsed: Optional[Any] = None
        if response is not None:
            try:
                parsed = response.json()
            except ValueError:
                parsed = response.text[:2000] or None

        result = CamundaTriggerResult(
            ok=response is not None and 200 <= response.status_code < 300,
            request_type=request_type,
            url=url,
            status_code=response.status_code if response is not None else None,
            attempts=attempts,
            duration_ms=duration_ms,
            process_key=process_definition_key,
            message_name=message_name if request_type == "message" else None,
            business_key=business_key,
            variables=variables,
            response=parsed,
            error=error,
            rule_id=rule_id,
            marketplace=marketplace,
            signal_code=signal_code,
            signal_name=signal_name,
            execution_id=execution_id,
        )

        if result.ok:
            logger.info(
                "Camunda: %s sukses (rule=%s marketplace=%s signal=%s business_key=%s "
                "signal_name=%s execution_id=%s) HTTP %s dalam %dms",
                request_type, rule_id, marketplace, signal_code, business_key,
                signal_name, execution_id, result.status_code, duration_ms,
            )
        else:
            logger.error(
                "Camunda: %s GAGAL setelah %d percobaan (rule=%s, url=%s): %s",
                request_type, attempts, rule_id, url, error,
            )
        return result

    # ------------------------------------------------------------------
    # Convenience wrappers
    # ------------------------------------------------------------------

    async def start_process_instance(
        self,
        process_definition_key: str,
        variables: Dict[str, Any],
        variable_types: Optional[Dict[str, str]] = None,
        business_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        **meta: Any,
    ) -> CamundaTriggerResult:
        """Start process instance berdasarkan process definition key."""
        return await self.trigger(
            request_type="start_process",
            variables=variables,
            variable_types=variable_types,
            process_definition_key=process_definition_key,
            business_key=business_key,
            tenant_id=tenant_id,
            extra_body=extra_body,
            **meta,
        )

    async def send_signal(
        self,
        signal_name: str,
        variables: Optional[Dict[str, Any]] = None,
        variable_types: Optional[Dict[str, str]] = None,
        tenant_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        **meta: Any,
    ) -> CamundaTriggerResult:
        """Kirim SIGNAL ke Camunda (v7 broadcast /signal, v8 /signals/broadcast).

        ``execution_id`` (Camunda 7 saja) membatasi sinyal ke SATU eksekusi
        (POST /execution/{id}/signal). Camunda 8 tidak mendukungnya.
        """
        return await self.trigger(
            request_type="signal",
            variables=variables or {},
            variable_types=variable_types,
            tenant_id=tenant_id,
            extra_body=extra_body,
            signal_name=signal_name,
            execution_id=execution_id,
            **meta,
        )

    async def send_message(
        self,
        message_name: str,
        variables: Dict[str, Any],
        variable_types: Optional[Dict[str, str]] = None,
        business_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        **meta: Any,
    ) -> CamundaTriggerResult:
        """Kirim message event (Camunda 7 /engine-rest/message)."""
        return await self.trigger(
            request_type="message",
            variables=variables,
            variable_types=variable_types,
            message_name=message_name,
            business_key=business_key,
            tenant_id=tenant_id,
            extra_body=extra_body,
            **meta,
        )
