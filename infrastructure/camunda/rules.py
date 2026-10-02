"""
Loader + matcher + mapper ATURAN webhook → Camunda.

File aturan (default: camunda/config.json, bisa dipindah lewat env
CAMUNDA_RULES_FILE) menentukan, untuk setiap webhook yang masuk:

  1. Aturan mana yang cocok  → marketplace / signal_codes / conditions
  2. Proses apa yang dijalankan → process_definition_key / message_name
  3. Variabel apa yang dikirim  → variables (dot-path, template, literal)

File dibaca ULANG setiap kali dipakai (di-cache berdasarkan mtime) sehingga
edit config.json langsung berlaku tanpa restart service — mengikuti pola
apisix/apisix/webhook-config.json di sisi gateway.

BENTUK A — FLAT / DINAMIS (disarankan, dipakai camunda/config.json)
-------------------------------------------------------------------
Tanpa `rules`: seluruh isi file adalah SATU aturan. Semua kunci boleh dinamis,
array dipakai sebagai rantai fallback antar marketplace/versi API:

    {
      "process_definition_key": "Mirorim_Operasional.Trigger_Retur",
      "business_key": "{{var.resi}}:{{var.courier}}:{{var.retur_date}}",
      "variables": {
        "resi":    ["data.tracking_number", "data.ordersn", "data.order_id"],
        "courier": ["data.shipping_carrier", "data.courier", {"literal": ""}],
        "marketplace": "{{marketplace}}"
      },
      "variable_types": {"resi": "string"}
    }

Setiap webhook yang masuk dijalankan (TIDAK ada filter marketplace/signal code),
KECUALI gate status order opsional `order_status` / `exclude_order_status` —
lihat bagian "Gate STATUS ORDER" di bawah. `process_definition_key` juga boleh
array → satu instance per key.

BENTUK B — RULES BERFILTER (opsional, masih didukung)
-----------------------------------------------------
    {
      "id": "shopee-order-status",
      "enabled": true,
      "marketplace": "shopee",           // "all" | "shopee" | ["shopee","tiktok"]
      "signal_codes": [3],               // "all" | 3 | [3, 1]
      "exclude_codes": [],
      "order_status": ["UNPAID", "READY_TO_SHIP"],   // gate status (opsional)
      "exclude_order_status": ["CANCELLED"],
      "conditions": [ {"path": "data.status", "op": "eq", "value": "READY_TO_SHIP"} ],
      "condition_mode": "all",           // "all" | "any"
      "type": "start_process",           // start_process | start_process_by_id | message
      "process_definition_key": "order-status-process",
      "message_name": "",                // diisi bila type = message (v7)
      "business_key": "{{var.resi}}:{{var.courier}}:{{data.ordersn}}",  // {{var.*}} = variabel hasil mapping
      "variables": {
        "orderId":    {"path": "data.ordersn"},
        "orderStatus":{"path": "data.status"},
        "marketplace": "{{marketplace}}",
        "source":     {"literal": "apisix-webhook"}
      },
      "variable_types": {"orderQty": "Long", "resi": "string"}
      // opsional (v7). Nama tipe boleh lowercase ("string"/"long") —
      // dinormalisasi, dan nilai diselaraskan dengan tipe tsb.
    }

Mode pencocokan (level atas): "mode": "all" (semua rule cocok dijalankan —
satu webhook boleh start beberapa proses) atau "first" (rule pertama saja).
Default dari env CAMUNDA_MATCH_MODE. Hanya relevan untuk BENTUK B.

GATE STATUS ORDER (opsional)
----------------------------
    "order_status":         ["UNPAID", "ON_HOLD", "AWAITING_SHIPMENT"],
    "exclude_order_status": ["CANCELLED"],

Status dibaca dari payload WEBHOOK MENTAH (Shopee `data.status`, TikTok
`data.order_status` — lihat camunda/order_detail.py::extract_webhook_order_status)
dan masuk ke konteks sebagai `meta.order_status`. Webhook dengan status di luar
daftar TIDAK men-start proses (log `skipped_reason: order_status_not_allowed`).

Gate GLOBAL tambahan ada di `WEBHOOK_ORDER_STATUS_ALLOWLIST` (.env, dipisah koma)
dan berlaku sebagai LANTAI KERAS: nilai di aturan hanya bisa mempersempit,
bukan melonggarkan. Kosong = tidak ada gate global.

BENTUK SPESIFIKASI NILAI VARIABEL
---------------------------------
    {"path": "data.ordersn"}                  → ambil dari payload (skip bila tidak ada)
    {"path": "data.status", "default": "-"}   → pakai default bila tidak ada
    {"literal": 12}                           → nilai tetap apa pun tipe-nya
    {"template": "{{marketplace}}-{{data.ordersn}}"}
    {"first": [ {"path": "data.ordersn"}, {"path": "data.order_id"} ]}  → fallback berantai
    ["data.ordersn", "data.order_id", {"literal": ""}]                  → SAMA dengan di atas (array)
    "{{data.ordersn}}"                        → template (placeholder tunggal menjaga tipe)
    "data.ordersn"                            → dot-path payload (skip bila tidak ada)
    "shop_id"                                 → kunci level atas payload (skip bila tidak ada)
    (untuk nilai tetap gunakan {"literal": "..."} — jangan string polos)
    12 / true                                 → literal (non-string, tanpa ambiguitas)
    {"anyKey": "bebas"}                       → literal objek (tanpa key path/literal/template/first)

PLACEHOLDER {{...}} YANG TERSEDIA
---------------------------------
    {{marketplace}}        {{signal_code}}     {{received_at}}     {{order_status}}
    {{headers.X-Webhook-Target}}               (nama header apa pun)
    {{data.ordersn}}                           (dot-path payload mentah)
    {{order.products_summary}}                 (detail order dari APISIX — lihat
                                                bagian DETAIL ORDER di bawah)

DETAIL ORDER DARI APISIX (namespace `order.*`)
----------------------------------------------
Sebelum start event, backend memanggil APISIX (GET /order?ids=<order id>) dan
menyimpan hasilnya di konteks `order` — TIDAK di dalam payload webhook. Path
yang diawali `order.` (alias `order_detail.` / `detail.`) di-resolve dari
konteks itu, sehingga variabel Camunda bisa memuat produk yang di-order:

    {
      "variables": {
        "orderItems":         {"path": "order.items"},           // JSON array
        "orderProducts":      {"path": "order.products_summary"}, // "Kaos x2, Celana x1"
        "orderTotalQuantity": {"path": "order.total_quantity"},   // 3
        "orderDetailOk":      {"path": "order.ok"}                // true/false
      }
    }

Kunci yang tersedia di konteks `order`: ok, source, marketplace, order_id,
shop_uuid, order_status, items[], item_count, total_quantity, product_names[],
products_summary, error.
"""

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from infrastructure.camunda.variables import normalize_variable_type
from core.config import settings

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_TEMPLATE_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")

# Key spesifikasi nilai variabel yang dikenali (selain ini → literal objek).
_SPEC_KEYS = ("path", "literal", "template", "first", "default")

# Prefix path yang di-resolve dari konteks DETAIL ORDER (hasil fetch APISIX),
# bukan dari payload webhook. Contoh: "order.items", "order.total_quantity".
ORDER_PATH_PREFIXES = ("order.", "order_detail.", "detail.")
ORDER_ROOT_NAMES = ("order", "order_detail", "detail")


# ---------------------------------------------------------------------------
# Path file & loader
# ---------------------------------------------------------------------------


def rules_file_path() -> Path:
    """Path absolut file aturan Camunda (relatif root project bila perlu)."""
    path = Path(settings.camunda_rules_file)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _normalize_rules(raw: Any) -> Dict[str, Any]:
    """Normalisasi config: semua kunci dipertahankan, defaults/rules dirapikan.

    Kunci lain (process_definition_key, variables, business_key, mode, ...)
    sengaja TIDAK dibuang — bentuk "flat" (tanpa `rules`) memakai kunci-kunci
    level atas itu langsung sebagai satu aturan dinamis.
    """
    config = dict(raw) if isinstance(raw, dict) else {}
    if not isinstance(config.get("defaults"), dict):
        config["defaults"] = {}
    raw_rules = config.get("rules")
    if not isinstance(raw_rules, list):
        raw_rules = []
    config["rules"] = [rule for rule in raw_rules if isinstance(rule, dict)]
    config.setdefault("version", "1.0.0")
    return config


_cache: Dict[str, Any] = {"path": None, "mtime": None, "config": None}


def load_rules(force: bool = False) -> Dict[str, Any]:
    """Baca file aturan (cached by mtime — edit file langsung berlaku)."""
    path = rules_file_path()

    try:
        mtime = path.stat().st_mtime
    except OSError:
        # File tidak ada → config KOSONG (jangan pernah pakai cache dari file
        # lain, supaya perilaku "env-only" tidak ketularan rules lain).
        if _cache["path"] != str(path) or _cache["config"] is None or force:
            logger.warning(
                "Camunda: file aturan tidak ditemukan (%s) — memakai defaults kosong",
                path,
            )
        _cache.update({"path": str(path), "mtime": None, "config": _normalize_rules({})})
        return _cache["config"]

    if (
        not force
        and _cache["config"] is not None
        and _cache["path"] == str(path)
        and _cache["mtime"] == mtime
    ):
        return _cache["config"]

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("Camunda: gagal membaca aturan dari %s: %s", path, exc)
        _cache.update({"path": str(path), "mtime": None, "config": _normalize_rules({})})
        return _cache["config"]

    config = _normalize_rules(raw)
    _cache["path"] = str(path)
    _cache["mtime"] = mtime
    _cache["config"] = config
    logger.info(
        "Camunda: aturan dimuat (%d rule) dari %s", len(config["rules"]), path
    )
    return config


# ---------------------------------------------------------------------------
# Dot-path & template helpers
# ---------------------------------------------------------------------------


def get_path(root: Any, path: Optional[str]) -> Any:
    """Ambil nilai dari tabel/dict berdasarkan dot-path (\"data.ordersn\")."""
    if path is None or path == "":
        return None
    current = root
    for segment in str(path).split("."):
        if isinstance(current, dict):
            if segment in current:
                current = current[segment]
                continue
            # dukung index angka untuk list
            if segment.lstrip("-").isdigit() and current.get(int(segment)) is not None:
                current = current[int(segment)]
                continue
            return None
        if isinstance(current, (list, tuple)) and segment.lstrip("-").isdigit():
            index = int(segment)
            if -len(current) <= index < len(current):
                current = current[index]
                continue
            return None
        return None
    return current


def _order_context(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Konteks detail order (hasil fetch APISIX) — dict kosong bila tidak ada."""
    order = ctx.get("order")
    return order if isinstance(order, dict) else {}


def resolve_path(path: Optional[str], ctx: Dict[str, Any]) -> Any:
    """Resolve dot-path variabel ke KONTEKS yang tepat.

    - `order.*` / `order_detail.*` / `detail.*` → konteks detail order dari
      APISIX (produk apa saja yang di-order + jumlahnya).
    - selain itu → payload webhook mentah (`ctx["body"]`).
    """
    text = str(path or "")
    if text in ORDER_ROOT_NAMES:
        return _order_context(ctx)
    for prefix in ORDER_PATH_PREFIXES:
        if text.startswith(prefix):
            return get_path(_order_context(ctx), text[len(prefix):])
    return get_path(ctx.get("body"), text)


def _lookup(path: str, ctx: Dict[str, Any]) -> Any:
    """Cari nilai untuk placeholder.

    Urutan: meta (marketplace/signal_code/received_at) → headers.* →
    var.* / variables.* (variabel hasil mapping) → payload mentah.

    Prefix `var.` dipakai untuk merujuk variabel yang BARU di-resolve dari
    `variables`, mis. business key "{{var.resi}}:{{var.courier}}" — jadi
    business key selalu konsisten dengan isi variabel yang dikirim.
    """
    meta = ctx.get("meta") or {}
    if path in meta:
        return meta[path]

    # Detail order dari APISIX (namespace order.*) — dicek SEBELUM headers
    # supaya nama header yang kebetulan berawalan "order" tidak menang.
    if path in ORDER_ROOT_NAMES or path.startswith(ORDER_PATH_PREFIXES):
        return resolve_path(path, ctx)

    if path.startswith("headers.") or path.startswith("header."):
        header_name = path.split(".", 1)[1]
        headers = ctx.get("headers") or {}
        for key, value in headers.items():
            if key.lower() == header_name.lower():
                return value
        return None

    if path.startswith("variables.") or path.startswith("var."):
        name = path.split(".", 1)[1]
        variables = ctx.get("variables") or {}
        return variables.get(name)

    return get_path(ctx.get("body"), path)


def render_template(value: Any, ctx: Dict[str, Any]) -> Any:
    """Render placeholder {{path}}.

    Placeholder TUNGGAL mempertahankan tipe asli (angka tetap angka, boolean
    tetap boolean, objek tetap objek) supaya variabel Camunda tetap bertipe
    benar. Placeholder campur teks menghasilkan string.
    """
    if not isinstance(value, str) or "{{" not in value:
        return value

    single = _TEMPLATE_RE.fullmatch(value.strip())
    if single:
        return _lookup(single.group(1).strip(), ctx)

    def _replace(match: "re.Match") -> str:
        resolved = _lookup(match.group(1).strip(), ctx)
        if resolved is None:
            return ""
        if isinstance(resolved, (dict, list)):
            return json.dumps(resolved, ensure_ascii=False)
        if isinstance(resolved, bool):
            return "true" if resolved else "false"
        return str(resolved)

    return _TEMPLATE_RE.sub(_replace, value)


def render_deep(value: Any, ctx: Dict[str, Any]) -> Any:
    """Render placeholder secara rekursif ke dict/list."""
    if isinstance(value, dict):
        return {key: render_deep(item, ctx) for key, item in value.items()}
    if isinstance(value, list):
        return [render_deep(item, ctx) for item in value]
    return render_template(value, ctx)


# ---------------------------------------------------------------------------
# Matching (marketplace / signal code / conditions)
# ---------------------------------------------------------------------------


def matches_value(rule_value: Any, actual: Any) -> bool:
    """Cocokkan nilai aturan (\"all\" | skalar | list) dengan nilai aktual."""
    if rule_value is None:
        return True
    if isinstance(rule_value, bool):
        return rule_value is True
    if isinstance(rule_value, str):
        low = rule_value.strip().lower()
        if low in ("", "all", "*"):
            return True
        return low == str(actual if actual is not None else "").lower()
    if isinstance(rule_value, (int, float)):
        return str(rule_value) == str(actual)
    if isinstance(rule_value, (list, tuple)):
        return any(matches_value(item, actual) for item in rule_value)
    return False


def _code_set(value: Any) -> Optional[set]:
    """Normalisasi daftar signal code → set; None = tanpa filter."""
    if value is None:
        return None
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("", "all", "*"):
            return None
        return {low}
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return {str(value)}
    if isinstance(value, (list, tuple)):
        codes = {
            str(item).strip().lower()
            for item in value
            if str(item).strip().lower() not in ("", "all", "*")
        }
        return codes or None
    return None


def codes_match(rule: Dict[str, Any], signal_code: Any) -> bool:
    """Cek include (signal_codes/codes) & exclude (exclude_codes/exclude)."""
    include = rule.get("signal_codes", rule.get("codes"))
    exclude = rule.get("exclude_codes", rule.get("exclude"))
    key = None if signal_code is None else str(signal_code).strip().lower()

    include_set = _code_set(include)
    if include_set and (key is None or key not in include_set):
        return False

    exclude_set = _code_set(exclude)
    if exclude_set and key is not None and key in exclude_set:
        return False

    return True


def parse_status_list(value: Any) -> Optional[set]:
    """Normalisasi daftar status order → set lookup huruf kecil.

    Menerima list JSON (`camunda/config.json`) MAUPUN string dipisah koma
    (`WEBHOOK_ORDER_STATUS_ALLOWLIST=UNPAID,ON_HOLD`) supaya setting .env enak
    dibaca. None/""/"all"/"*" = TANPA filter (semua status lolos).
    """
    if isinstance(value, str) and "," in value:
        value = value.split(",")
    return _code_set(value)


def order_status_matches(rule: Dict[str, Any], order_status: Any) -> bool:
    """Cek include (`order_status`) & exclude (`exclude_order_status`) aturan.

    Contoh di camunda/config.json:
        "order_status":         ["UNPAID", "ON_HOLD", "AWAITING_SHIPMENT"],
        "exclude_order_status": ["CANCELLED"]
    """
    include = rule.get("order_status", rule.get("statuses"))
    exclude = rule.get("exclude_order_status", rule.get("exclude_statuses"))
    key = None if order_status is None else str(order_status).strip().lower()

    include_set = parse_status_list(include)
    if include_set and (key is None or key not in include_set):
        return False

    exclude_set = parse_status_list(exclude)
    if exclude_set and key is not None and key in exclude_set:
        return False

    return True


def _to_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _values_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    num_left, num_right = _to_number(left), _to_number(right)
    if num_left is not None and num_right is not None:
        return num_left == num_right
    return str(left) == str(right)


def evaluate_condition(condition: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """Evaluasi satu kondisi {path, op, value} terhadap payload mentah."""
    path = condition.get("path", condition.get("field"))
    op = str(condition.get("op", condition.get("operator", "eq"))).lower()
    expected = condition.get("value", condition.get("equals"))
    actual = get_path(ctx.get("body"), path)

    if op == "exists":
        return actual is not None
    if op == "missing":
        return actual is None
    if op in ("eq", "equals"):
        return _values_equal(actual, expected)
    if op in ("ne", "not_equals"):
        return not _values_equal(actual, expected)
    if op in ("gt", "gte", "lt", "lte"):
        num_actual, num_expected = _to_number(actual), _to_number(expected)
        if num_actual is None or num_expected is None:
            return False
        if op == "gt":
            return num_actual > num_expected
        if op == "gte":
            return num_actual >= num_expected
        if op == "lt":
            return num_actual < num_expected
        return num_actual <= num_expected
    if op == "contains":
        if isinstance(actual, (list, tuple)):
            return any(_values_equal(item, expected) for item in actual)
        if actual is None:
            return False
        return str(expected) in str(actual)
    if op == "in":
        return matches_value(expected, actual)
    if op == "nin":
        return not matches_value(expected, actual)
    if op in ("regex", "match"):
        if actual is None:
            return False
        try:
            return re.search(str(expected), str(actual)) is not None
        except re.error:
            return False

    logger.warning("Camunda: operator kondisi '%s' tidak dikenal — diabaikan", op)
    return True


def conditions_pass(rule: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """Cek seluruh kondisi aturan (mode \"all\" default, \"any\" bila diminta)."""
    conditions = rule.get("conditions")
    if not isinstance(conditions, list) or not conditions:
        return True

    mode = str(rule.get("condition_mode", "all")).lower()
    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        result = evaluate_condition(condition, ctx)
        if mode == "any" and result:
            return True
        if mode != "any" and not result:
            return False
    return mode != "any"


def rule_matches(rule: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """Apakah satu aturan cocok dengan konteks webhook (`ctx`)."""
    if rule.get("enabled") is False:
        return False

    meta = ctx.get("meta") or {}
    if not matches_value(rule.get("marketplace"), meta.get("marketplace")):
        return False

    if "signal_codes" in rule or "signal_code" in rule or "codes" in rule:
        if not codes_match(rule, meta.get("signal_code")):
            return False

    # Gate status order (opsional per aturan). `meta.order_status` diisi dari
    # payload webhook mentah — lihat camunda/order_detail.py.
    if (
        "order_status" in rule
        or "statuses" in rule
        or "exclude_order_status" in rule
        or "exclude_statuses" in rule
    ):
        if not order_status_matches(rule, meta.get("order_status")):
            return False

    return conditions_pass(rule, ctx)


def select_rule(config: Dict[str, Any], ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Aturan pertama (urutan di file menentukan prioritas) yang cocok."""
    for rule in config.get("rules", []):
        if rule_matches(rule, ctx):
            return rule
    return None


def resolve_match_mode(config: Dict[str, Any]) -> str:
    """Mode pencocokan rule dari config.json.

    - "all"   (default): SEMUA rule yang cocok dijalankan — satu webhook bisa
                men-start beberapa proses sekaligus.
    - "first"          : hanya rule pertama yang cocok (perilaku lama).

    Boleh ditulis di level atas ("mode") atau di dalam "settings".
    """
    mode = config.get("mode")
    if mode in (None, ""):
        settings_block = config.get("settings")
        if isinstance(settings_block, dict):
            mode = settings_block.get("mode")
    if mode in (None, ""):
        mode = settings.camunda_match_mode
    normalized = str(mode).strip().lower()
    return normalized if normalized in ("all", "first") else "all"


# ---------------------------------------------------------------------------
# Resolusi variabel
# ---------------------------------------------------------------------------


def _resolve_spec(spec: Any, ctx: Dict[str, Any]) -> Tuple[Any, bool]:
    """Ubah satu spesifikasi menjadi nilai.

    @return (nilai, ditemukan) — `ditemukan=False` berarti variabel di-SKIP.
    """
    if isinstance(spec, dict):
        has_spec_key = any(key in spec for key in _SPEC_KEYS)
        if not has_spec_key:
            # objek literal (mis. {"a": 1}) — bukan spesifikasi
            return render_deep(spec, ctx), True

        if "first" in spec and isinstance(spec["first"], list):
            for candidate in spec["first"]:
                value, found = _resolve_spec(candidate, ctx)
                if found and value is not None:
                    return value, True
            # tidak ada yang kena → fallback default bila ada
            if "default" in spec:
                return render_deep(spec["default"], ctx), True
            return None, False

        if "literal" in spec:
            return render_deep(spec["literal"], ctx), True

        if "template" in spec:
            rendered = render_template(str(spec["template"]), ctx)
            if rendered is None:
                if "default" in spec:
                    return render_deep(spec["default"], ctx), True
                return None, False
            return rendered, True

        if "path" in spec:
            value = resolve_path(spec["path"], ctx)
            if value is None:
                if "default" in spec:
                    return render_deep(spec["default"], ctx), True
                return None, False
            return value, True

        # hanya berisi "default"
        if "default" in spec:
            return render_deep(spec["default"], ctx), True
        return None, False

    if isinstance(spec, (list, tuple)):
        # ARRAY = rantai fallback (dibaca kiri → kanan, yang pertama ada dipakai).
        # Contoh: ["data.tracking_number", "data.ordersn", {"literal": ""}]
        # Untuk mengirim ARRAY sebagai NILAI variabel, bungkus:
        #   {"literal": ["a", "b"]}
        for candidate in spec:
            value, found = _resolve_spec(candidate, ctx)
            if found and value is not None:
                return value, True
        return None, False

    if isinstance(spec, str):
        if "{{" in spec:
            rendered = render_template(spec, ctx)
            return (rendered, rendered is not None)
        # String polos SELALU dianggap path (dot-path payload, kunci level atas
        # seperti "shop_id", atau namespace detail order "order.items"). Bila
        # tidak ada → variabel dilewati. Ini disengaja: kalau string tanpa titik
        # dianggap literal, salah tulis/nama field yang tidak ada akan diam-diam
        # terkirim sebagai nilai literal (mis. retur_date = "timestamp").
        # Untuk nilai tetap, tulis eksplisit: {"literal": "..."}.
        value = resolve_path(spec, ctx)
        return (value, value is not None)

    if spec is None:
        return None, False

    return spec, True


def resolve_variables(
    rules_map: Dict[str, Any],
    defaults_map: Dict[str, Any],
    ctx: Dict[str, Any],
) -> Dict[str, Any]:
    """Gabung defaults.variables + rule.variables → map variabel final.

    Variabel dari aturan MENIMPA default. Variabel yang nilainya tidak
    ditemukan (path kosong tanpa default) otomatis dilewati.
    """
    merged: Dict[str, Any] = {}
    if isinstance(defaults_map, dict):
        merged.update(defaults_map)
    if isinstance(rules_map, dict):
        merged.update(rules_map)

    resolved: Dict[str, Any] = {}
    for name, spec in merged.items():
        if str(name).startswith("_"):
            continue
        value, found = _resolve_spec(spec, ctx)
        if not found or value is None:
            logger.debug("Camunda: variabel '%s' dilewati (nilai tidak ada)", name)
            continue
        resolved[name] = value
    return resolved


def resolve_variable_types(defaults: Dict[str, Any], rule: Dict[str, Any]) -> Dict[str, str]:
    """Gabung variable_types dari defaults + rule.

    Nama tipe dinormalisasi ke bentuk kanonik Camunda, sehingga config boleh
    memakai gaya dokumentasi REST yang lowercase ("string", "long").
    """
    types: Dict[str, str] = {}
    for source in (defaults.get("variable_types"), rule.get("variable_types")):
        if isinstance(source, dict):
            for name, type_name in source.items():
                if not str(name).startswith("_"):
                    types[name] = normalize_variable_type(type_name)
    return types
