"""
DTO hasil trigger webhook → Camunda.

Dipisah dari client & service supaya satu bentuk hasil dipakai konsisten oleh
router (response HTTP), service (log file), dan test.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class CamundaTriggerResult:
    """Hasil satu percobaan trigger Camunda dari webhook.

    Attributes:
        ok              : True bila Camunda membalas 2xx.
        request_type    : \"start_process\" | \"start_process_by_id\" | \"message\".
        url             : URL lengkap yang dipanggil (termasuk base + path).
        status_code     : Status HTTP Camunda (None bila gagal koneksi/skip).
        attempts        : Jumlah percobaan yang dilakukan (termasuk retry).
        duration_ms     : Lama total seluruh percobaan (ms).
        process_key     : Process definition key yang dipakai (bila ada).
        message_name    : Message name (bila tipe message).
        business_key    : Business key yang dikirim (bila ada).
        variables       : Variabel yang dikirim ke Camunda (nilai final).
        response        : Body JSON balasan Camunda (bila bisa di-decode).
        error           : Pesan error terakhir (bila gagal).
        skipped_reason  : Alasan trigger tidak dilakukan (mis. \"camunda_disabled\",
                          \"no_matching_rule\") — `ok` akan True karena bukan error.
        rule_id         : id aturan di camunda/config.json yang dipakai.
        marketplace     : Marketplace asal webhook.
        signal_code     : Kode signal webhook (shopee: code, tiktok: type).
        signal_name     : Nama SIGNAL Camunda yang dikirim (request_type=signal).
        execution_id    : Execution target bila sinyal dikirim ke satu eksekusi
                          saja (Camunda 7: /execution/{id}/signal).
    """

    ok: bool
    request_type: str = "skip"
    url: str = ""
    status_code: Optional[int] = None
    attempts: int = 0
    duration_ms: int = 0
    process_key: Optional[str] = None
    message_name: Optional[str] = None
    business_key: Optional[str] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    response: Optional[Any] = None
    error: Optional[str] = None
    skipped_reason: Optional[str] = None
    rule_id: Optional[str] = None
    marketplace: Optional[str] = None
    signal_code: Optional[Any] = None
    signal_name: Optional[str] = None
    execution_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Bentuk aman-JSON untuk log & response ringkas."""
        return {
            "ok": self.ok,
            "request_type": self.request_type,
            "url": self.url,
            "status_code": self.status_code,
            "attempts": self.attempts,
            "duration_ms": self.duration_ms,
            "process_key": self.process_key,
            "message_name": self.message_name,
            "business_key": self.business_key,
            "variables": self.variables,
            "response": self.response,
            "error": self.error,
            "skipped_reason": self.skipped_reason,
            "rule_id": self.rule_id,
            "marketplace": self.marketplace,
            "signal_code": self.signal_code,
            "signal_name": self.signal_name,
            "execution_id": self.execution_id,
        }


@dataclass
class CamundaPlan:
    """Rencana request hasil resolusi aturan (dipakai internal service/client).

    Attributes:
        request_type         : start_process | start_process_by_id | message
        process_definition_key / process_definition_id / message_name
        variables            : map nama variabel → nilai (sudah di-render)
        variable_types       : override tipe variabel Camunda 7 (mis. Long)
        business_key / tenant_id / extra_body
        rule_id              : id aturan sumber (untuk log)
    """

    request_type: str = "start_process"
    process_definition_key: Optional[str] = None
    process_definition_id: Optional[str] = None
    message_name: Optional[str] = None
    variables: Dict[str, Any] = field(default_factory=dict)
    variable_types: Dict[str, str] = field(default_factory=dict)
    business_key: Optional[str] = None
    tenant_id: Optional[str] = None
    extra_body: Dict[str, Any] = field(default_factory=dict)
    rule_id: Optional[str] = None
    # type=signal: nama sinyal + (opsional, Camunda 7) execution target.
    signal_name: Optional[str] = None
    execution_id: Optional[str] = None
