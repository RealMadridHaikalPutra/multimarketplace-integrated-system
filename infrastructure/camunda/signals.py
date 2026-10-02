"""
SIGNAL Camunda — pemetaan STATUS ORDER → nama signal yang dikirim ("hit signal").

Kenapa terpisah dari `rules.py`?
  Start event (`start_process` / `message`) memakai `variables` + `business_key`
  di level atas config.json, sedangkan SIGNAL punya kebutuhan sendiri: satu nama
  sinyal PER STATUS ORDER (dan boleh beda per marketplace). Pengaturannya ada di
  bagian `signal` pada camunda/config.json dan dibaca ULANG otomatis (hot reload)
  karena memakai loader yang sama (`rules.load_rules`).

Bentuk konfigurasi (lihat camunda/config.json):

    "signal": {
      "enabled": true,
      "send_on_webhook": false,        // true = otomatis dikirim saat webhook masuk
      "by_status": {                   // STATUS ORDER → nama sinyal Camunda
        "ON_HOLD": "retur-on-hold",
        "UNPAID": "retur-unpaid"
      },
      "by_marketplace": {              // opsional, MENIMPA by_status
        "tiktok": {"ON_HOLD": "retur-on-hold-tiktok"}
      },
      "default_signal": "",            // dipakai bila status tidak dipetakan
      "execution_id": null,            // Camunda 7: kirim ke SATU eksekusi saja
      "business_key": "",              // C7 /signal tidak punya businessKey →
                                       // nilainya dikirim sebagai variabel
      "status_fallback": {             // status DETAIL ORDER (APISIX) dipakai
        "enabled": true,               //   bila status WEBHOOK tak dipetakan
        "skip_statuses": []            //   (mis. UNPAID yang hanya untuk START)
      },
      "variables": {"signalName": {"literal": "..."} },   // sintaks sama rule
      "variable_types": {"orderId": "string"}
    }

Presedensi resolusi nama sinyal:
  1. ``by_marketplace[<marketplace>][<STATUS>]``  (paling spesifik)
  2. ``by_status[<STATUS>]``
  3. ``default_signal`` (bila diisi)

Placeholder pada NAMA sinyal (`by_status` / `by_marketplace` /
``default_signal``) — CONTOH nyata: kadang ada nilai seperti
``"readyToShip${invoice}"`` yang artinya nama sinyalnya DINAMIS per order.
Nilai placeholder diisi dari VARIABEL signal hasil mapping (`signal.variables`
+ argumen) memakai sintaks ``${invoice}`` (gaya JS template literal) ATAU
``{{invoice}}`` — keduanya sama artinya, dan awalan ``var.`` boleh ditulis
(``${var.invoice}`` == ``${invoice}``). Render dilakukan di
``service.send_signal`` / ``service._plan_from_rule`` via
:func:`render_signal_name` SEBELUM nama dikirim ke Camunda.
Placeholder yang variabelnya tidak ditemukan DIBIARKAN apa adanya + warning
di log (supaya kelihatan di log bahwa mappingnya kurang, bukan namanya salah).

Status dibandingkan case-insensitive (spasi tepi dibuang), jadi
``data.status`` Shopee (huruf besar) maupun ``data.order_status`` TikTok
sama-sama cocok dengan kunci ``ON_HOLD``.
"""

import json
import logging
import re
from typing import Any, Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Kunci di camunda/config.json yang memuat konfigurasi signal.
SIGNAL_CONFIG_KEY = "signal"


def signal_config(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Ambil bagian `signal` dari config.json (dict kosong bila tidak ada).

    ``config`` boleh dikirim oleh pemanggil yang sudah memuat rules (menghindari
    pembacaan file kedua); None → dibaca di sini via `rules.load_rules()`
    (hot reload: edit config.json → langsung berlaku).
    """
    if config is None:
        from infrastructure.camunda.rules import load_rules  # import lokal: hindari siklus

        config = load_rules()
    section = (config or {}).get(SIGNAL_CONFIG_KEY)
    return section if isinstance(section, dict) else {}


def signal_is_enabled(cfg: Dict[str, Any]) -> bool:
    """False HANYA bila `signal.enabled` eksplisit false."""
    return (cfg or {}).get("enabled") is not False


def signal_send_on_webhook(cfg: Dict[str, Any]) -> bool:
    """True bila webhook masuk harus OTOMATIS mengirim signal (opsional)."""
    return bool((cfg or {}).get("send_on_webhook"))


def execution_lookup_config(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Bagian ``signal.execution_lookup`` (dict kosong bila tidak ada).

    Bentuk:

        "execution_lookup": {
          "enabled": true,
          "business_key_template": "{{var.invoice}}:%",
          "fallback": "broadcast"      // "broadcast" | "skip"
        }

    ``enabled=true`` membuat backend mencari execution yang benar-benar menunggu
    signal ini pada process instance dengan business key cocok LEBIH DULU, lalu
    mengirim ke ``POST /execution/{id}/signal`` (bukan broadcast ke semua instance).
    """
    return execution_lookup_section(signal_config(cfg))


def execution_lookup_section(section: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Bagian ``execution_lookup`` langsung dari SECTION ``signal``.

    Dipakai pemanggil yang sudah memegang dict bagian `signal` (mis.
    ``service.send_signal``) supaya tidak membungkus config dua kali.
    """
    raw = (section or {}).get("execution_lookup")
    return raw if isinstance(raw, dict) else {}


def execution_lookup_enabled(cfg: Optional[Dict[str, Any]] = None) -> bool:
    """True bila korelasi signal → execution spesifik diaktifkan."""
    return execution_lookup_config(cfg).get("enabled") is True


def status_fallback_section(section: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Bagian ``signal.status_fallback`` langsung dari SECTION ``signal``.

    Bentuk:

        "status_fallback": {
          "enabled": true,
          "skip_statuses": []          // status DETAIL yang tidak boleh dipakai
        }

    ``enabled=true`` → bila status dari payload WEBHOOK TIDAK punya mapping di
    ``by_status``/``by_marketplace``, status dari DETAIL ORDER APISIX
    (``order.order_status`` → variabel ``orderDetailStatus``) dipakai sebagai
    status EFEKTIF untuk memilih signal.
    """
    raw = (section or {}).get("status_fallback")
    return raw if isinstance(raw, dict) else {}


def status_fallback_config(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Bagian ``signal.status_fallback`` dari config.json (dict kosong bila ada)."""
    return status_fallback_section(signal_config(cfg))


def status_fallback_enabled(cfg: Optional[Dict[str, Any]] = None) -> bool:
    """True bila status DETAIL ORDER boleh dipakai sebagai status efektif."""
    return status_fallback_config(cfg).get("enabled") is True


def status_fallback_skip_statuses(
    section: Optional[Dict[str, Any]],
) -> Set[str]:
    """Status DETAIL yang DILARANG dipakai sebagai status efektif (UPPERCASE).

    ``skip_statuses`` (list/tuple/str dipisah koma) dipakai bila status dari
    gateway tidak boleh memicu signal tertentu — mis. cancel hanya boleh datang
    dari webhook, bukan dari hasil pembacaan detail order.
    """
    raw = status_fallback_section(section).get("skip_statuses")
    if isinstance(raw, str):
        items = raw.split(",")
    elif isinstance(raw, (list, tuple, set)):
        items = list(raw)
    else:
        return set()
    return {str(item).strip().upper() for item in items if str(item or "").strip()}


def _status_key(value: Any) -> str:
    """Normalisasi status order → kunci pembanding (huruf besar, tanpa spasi tepi)."""
    return str(value or "").strip().upper()


def _status_map(raw: Any) -> Dict[str, str]:
    """Normalisasi satu map status→signal (kunci uppercase, nilai string)."""
    result: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return result
    for status, name in raw.items():
        if str(status).startswith("_"):
            continue
        key = _status_key(status)
        value = str(name or "").strip()
        if key and value:
            result[key] = value
    return result


def marketplace_status_map(cfg: Dict[str, Any], marketplace: Any) -> Dict[str, str]:
    """Map status→signal khusus satu marketplace (`by_marketplace`)."""
    by_marketplace = (cfg or {}).get("by_marketplace")
    if not isinstance(by_marketplace, dict):
        return {}
    target = str(marketplace or "").strip().lower()
    if not target:
        return {}
    for name, raw in by_marketplace.items():
        if str(name).startswith("_"):
            continue
        # Kunci boleh "tiktok", "shopee", atau "all" (berlaku untuk semua).
        if str(name).strip().lower() in (target, "all"):
            merged: Dict[str, str] = {}
            merged.update(_status_map(raw))
            return merged
    return {}


def resolve_signal_name(
    cfg: Dict[str, Any],
    marketplace: Any = None,
    status: Any = None,
) -> Optional[str]:
    """Nama signal untuk (marketplace, status) — None bila tidak dipetakan.

    Presedensi: by_marketplace[mkt][STATUS] → by_status[STATUS] → default_signal.
    """
    if not isinstance(cfg, dict) or not cfg:
        return None

    status_key = _status_key(status)
    if status_key:
        per_marketplace = marketplace_status_map(cfg, marketplace)
        if status_key in per_marketplace:
            return per_marketplace[status_key]

        by_status = _status_map(cfg.get("by_status"))
        if status_key in by_status:
            return by_status[status_key]

    default_signal = str((cfg or {}).get("default_signal") or "").strip()
    return default_signal or None


def describe_signal_config(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Ringkasan konfigurasi signal untuk endpoint debug (tanpa mengirim apa pun)."""
    section = signal_config(cfg)
    by_status = _status_map(section.get("by_status"))
    by_marketplace_raw = section.get("by_marketplace")
    by_marketplace: Dict[str, Dict[str, str]] = {}
    if isinstance(by_marketplace_raw, dict):
        for name, raw in by_marketplace_raw.items():
            if str(name).startswith("_"):
                continue
            by_marketplace[str(name).strip().lower()] = _status_map(raw)

    return {
        "enabled": signal_is_enabled(section),
        "send_on_webhook": signal_send_on_webhook(section),
        "default_signal": str(section.get("default_signal") or "").strip() or None,
        "execution_id": str(section.get("execution_id") or "").strip() or None,
        "business_key": str(section.get("business_key") or "").strip() or None,
        "status_fallback": {
            "enabled": status_fallback_section(section).get("enabled") is True,
            "skip_statuses": sorted(status_fallback_skip_statuses(section)),
        },
        "execution_lookup": {
            "enabled": execution_lookup_section(section).get("enabled") is True,
            "business_key_template": str(
                execution_lookup_section(section).get("business_key_template") or ""
            ).strip() or None,
            "fallback": str(
                execution_lookup_section(section).get("fallback") or "broadcast"
            ).strip().lower(),
        },
        "by_status": by_status,
        "by_marketplace": by_marketplace,
        "statuses": sorted(by_status.keys()),
        "variables": sorted(
            name
            for name in (section.get("variables") or {})
            if not str(name).startswith("_")
        ),
        "variable_types": sorted(
            name
            for name in (section.get("variable_types") or {})
            if not str(name).startswith("_")
        ),
        "effective_map": {
            **(by_status),
            **{
                f"{marketplace}:{status}": name
                for marketplace, mapping in by_marketplace.items()
                for status, name in mapping.items()
            },
        },
        "skip_reason": "signal_not_configured",
    }


def resolve_signal_target(
    cfg: Dict[str, Any],
    marketplace: Any,
    status: Any,
) -> Tuple[Optional[str], str]:
    """Helper ringkas: (nama signal, alasan bila None).

    @return (nama, "") bila ketemu; (None, alasan) bila tidak.
            alasan = "signal_disabled" | "signal_not_configured"
    """
    if not signal_is_enabled(cfg):
        return None, "signal_disabled"
    name = resolve_signal_name(cfg, marketplace, status)
    if not name:
        return None, "signal_not_configured"
    return name, ""


# ---------------------------------------------------------------------------
# Render NAMA sinyal — placeholder ${...} / {{...}} dari VARIABEL signal
# ---------------------------------------------------------------------------

# Dua sintaks diterima untuk placeholder pada nama sinyal:
#   ${invoice}   gaya JS template literal (yang sering muncul di config)
#   {{invoice}}  sintaks placeholder yang sudah dipakai bagian config lain
_SIGNAL_NAME_PLACEHOLDER_RE = re.compile(r"\$\{([^{}]+)\}|\{\{\s*([^{}]+?)\s*\}\}")
# Awalan opsional supaya `${var.invoice}` dan `${invoice}` dianggap sama.
_SIGNAL_NAME_VAR_PREFIXES = ("var.", "variables.")
# Sentinel: bedakan "variabel tidak ada" dengan "nilainya None".
_SIGNAL_NAME_MISSING = object()


def _signal_name_value(expr: Any, variables: Dict[str, Any]) -> Any:
    """Cari nilai placeholder HANYA di dict variabel signal (bukan payload)."""
    key = str(expr or "").strip()
    for prefix in _SIGNAL_NAME_VAR_PREFIXES:
        if key.startswith(prefix):
            key = key[len(prefix) :]
            break
    if not key:
        return _SIGNAL_NAME_MISSING
    value = variables.get(key, _SIGNAL_NAME_MISSING)
    return _SIGNAL_NAME_MISSING if value is None else value


def _signal_name_text(value: Any) -> str:
    """Format nilai variabel jadi teks untuk disisipkan ke nama sinyal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def render_signal_name(
    name: Any,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    """Render placeholder nama sinyal dari VARIABEL signal.

    Mendukung ``${invoice}`` dan ``{{invoice}}`` pada nilai ``by_status`` /
    ``by_marketplace`` / ``default_signal`` (maupun ``signal_name`` rule/
    argumen manual). Sumber nilainya HANYA ``variables`` — yaitu hasil mapping
    ``signal.variables`` (+ argumen ``variables``), BUKAN payload mentah:

        "by_status": {"READY_TO_SHIP": "readyToShip${invoice}"}
        "variables": {"invoice": ["data.ordersn", "data.order_id"]}
        → render_signal_name("readyToShip${invoice}", {"invoice": "SH-1"})
        → "readyToShipSH-1"

    Aturan:
      - Tidak ada placeholder → nama dikembalikan apa adanya (tanpa salin).
      - Variabel tidak ketemu / nilainya None → placeholder DIBIARKAN utuh
        + ``warning`` di log (nama literal terlihat jelas saat di-debug).
      - Hasil render kosong (mis. ``"${invoice}"`` dengan nilai ``""``) →
        string kosong; pemanggil wajib men-skip kirim sinyal.
    """
    if not isinstance(name, str) or not name:
        return name
    if "${" not in name and "{{" not in name:
        return name

    vars_map = variables if isinstance(variables, dict) else {}
    missing: list = []

    def _replace(match: "re.Match") -> str:
        token = match.group(0)
        expr = match.group(1) if match.group(1) is not None else match.group(2)
        value = _signal_name_value(expr, vars_map)
        if value is _SIGNAL_NAME_MISSING:
            missing.append(str(expr).strip())
            return token
        return _signal_name_text(value)

    rendered = _SIGNAL_NAME_PLACEHOLDER_RE.sub(_replace, name)
    if missing:
        logger.warning(
            "Camunda: placeholder nama signal %s pada '%s' tidak menemukan "
            "variabel signal (mapping `signal.variables`) — dibiarkan apa adanya",
            missing, name,
        )
    return rendered
