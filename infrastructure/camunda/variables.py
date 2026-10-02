"""
Helper TIPE VARIABEL Camunda.

Dipisah dari client.py & rules.py supaya kedua modul memakai logika yang sama:

  normalize_variable_type("string")  → "String"   (config.json boleh lowercase)
  infer_variable_type({"a": 1})      → "Json"
  coerce_variable_value(2778101, "String") → "2778101"

Penting: Camunda 7 MEMVALIDASI pasangan nilai/tipe. Mengirim angka dengan
type "String" (atau string non-angka dengan type "Long") ditolak dengan
HTTP 400 — karena itu `coerce_variable_value` menyelaraskan nilai dengan tipe
yang diminta, dan pemanggil memakai inferensi bila penyelarasan mustahil.

Sama halnya untuk tipe "Json": Camunda 7 menolak nilai array/objek dengan
HTTP 400:

    Must provide 'null' or String value for value of SerializableValue type 'Json'.

Jadi nilai tipe Json HARUS berupa TEKS JSON (string). Helper
`serialize_json_value` mengonversinya otomatis supaya config.json boleh tetap
menulis orderItems/variabel objek apa adanya tanpa menggagalkan start event.
"""

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Tipe variabel Camunda 7 yang butuh nilai angka.
NUMERIC_VARIABLE_TYPES = {"Long", "Double", "Integer", "Short"}

# Nama tipe yang diterima di config.json → nama kanonik Camunda.
# (Dokumentasi REST Camunda sendiri memakai campuran "String"/"string".)
CANONICAL_VARIABLE_TYPES = {
    "string": "String",
    "str": "String",
    "text": "String",
    "long": "Long",
    "int": "Long",
    "integer": "Integer",
    "short": "Short",
    "double": "Double",
    "float": "Double",
    "number": "Double",
    "boolean": "Boolean",
    "bool": "Boolean",
    "json": "Json",
    "object": "Json",
    "date": "Date",
    "null": "Null",
}


def normalize_variable_type(name: Any) -> str:
    """Normalisasi nama tipe variabel ke bentuk kanonik Camunda.

    \"string\" → \"String\", \"json\" → \"Json\", nama tak dikenal dibiarkan
    apa adanya (biar Camunda yang menolak, bukan kita yang menebak).
    """
    raw = str(name or "").strip()
    if not raw:
        return "String"
    return CANONICAL_VARIABLE_TYPES.get(raw.lower(), raw)


def is_numeric_type(name: Any) -> bool:
    """True bila tipe (setelah normalisasi) butuh nilai angka."""
    return normalize_variable_type(name) in NUMERIC_VARIABLE_TYPES


def infer_variable_type(value: Any) -> str:
    """Tipe variabel Camunda 7 dari tipe Python."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Long"
    if isinstance(value, float):
        return "Double"
    if isinstance(value, (dict, list)):
        return "Json"
    return "String"


def coerce_variable_value(value: Any, declared_type: Any) -> Optional[Any]:
    """Konversi nilai agar SESUAI tipe yang dideklarasikan.

    @return nilai hasil konversi, atau None bila tidak mungkin dikonversi
            (pemanggil harus memakai tipe hasil inferensi sebagai gantinya).
    """
    if value is None:
        return None

    declared = normalize_variable_type(declared_type)

    if declared == "String":
        if isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    if declared in NUMERIC_VARIABLE_TYPES:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return value if declared != "Integer" or isinstance(value, int) else int(value)
        if isinstance(value, str):
            try:
                number = float(value)
            except ValueError:
                return None
            if declared == "Double":
                return number
            return int(number)
        return None

    if declared == "Boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        text = str(value).strip().lower()
        if text in ("true", "1", "yes"):
            return True
        if text in ("false", "0", "no"):
            return False
        return None

    if declared == "Json":
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return None
        return None

    # Tipe tidak dikenal (mis. Date) → jangan paksa; biarkan inferensi.
    return None


def serialize_json_value(value: Any, final_type: Any) -> Any:
    """Camunda 7 mewajibkan nilai tipe `Json` berupa TEKS JSON (string).

    Tanpa konversi ini, start process membalas HTTP 400 (terverifikasi):

        Must provide 'null' or String value for value of SerializableValue type 'Json'.

    Nilai yang sudah string (teks JSON) diteruskan apa adanya; dict/list
    di-serialize dengan ``ensure_ascii=False`` supaya nama produk non-ASCII
    (mis. bahasa Indonesia) tetap terbaca utuh di Camunda Cockpit.
    """
    if normalize_variable_type(final_type) != "Json":
        return value
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def build_typed_variable(value: Any, declared_type: Optional[Any] = None) -> tuple:
    """Bangun satu pasangan ({value, type}) untuk Camunda 7.

    Bila `declared_type` tidak bisa diselaraskan dengan nilainya, tipe yang
    diminta diabaikan dan dipakai tipe hasil inferensi (anti HTTP 400).
    Tipe `Json` selalu menerima nilai berupa teks JSON — lihat
    ``serialize_json_value``.

    @return (nilai_final, tipe_final)
    """
    if not declared_type:
        final_type = infer_variable_type(value)
        return serialize_json_value(value, final_type), final_type

    coerced = coerce_variable_value(value, declared_type)
    if coerced is None:
        final_type = infer_variable_type(value)
        logger.warning(
            "Camunda: tipe variabel diminta %s tapi nilai %r tidak bisa dikonversi "
            "— memakai tipe %s",
            normalize_variable_type(declared_type), value, final_type,
        )
        return serialize_json_value(value, final_type), final_type

    final_type = normalize_variable_type(declared_type)
    return serialize_json_value(coerced, final_type), final_type
