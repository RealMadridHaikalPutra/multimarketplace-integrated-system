"""

Central application configuration.

Single source of truth for every runtime setting. Values are loaded from
environment variables / ``.env`` (case-insensitive). This module replaces the
old root-level ``config.py`` and additionally includes the settings that the
Camunda / webhook subsystems read (which previously lived only in
``.env.example``), so the application can boot with every subsystem wired.

Rules:
- Never read secrets directly from code elsewhere; import ``settings`` here.
- Group settings by subsystem. Keep comments accurate to actual behaviour.
"""

from __future__ import annotations

from typing import Dict, List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Application
    # ------------------------------------------------------------------
    app_name: str = "Synchronization Service"
    app_version: str = "0.1.0"
    debug: bool = False

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------
    database_url: str = (
        "postgresql+asyncpg://postgres:Mamat.01@192.168.1.102:5434/sync_service"
    )
    database_echo: bool = False
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # ------------------------------------------------------------------
    # InvenTree (ERP / inventory source)
    # ------------------------------------------------------------------
    inventree_stock_view_url: str = (
        "https://mirorim.ddns.net:6789/graphql/api/rest/vwStockAvailableInventree"
    )
    # Snapshot stok per part: stock_available, stock_reserved_*, stok_buffer.
    inventree_stock_snapshot_url: str = (
        "https://mirorim.ddns.net:6789/graphql/api/rest/stockAvailableSnapshot"
    )
    inventree_api_url: str = "http://180.242.206.202:8004"
    inventree_api_token: str = ""

    # ------------------------------------------------------------------
    # APISIX (marketplace gateway)
    #
    # Backend communicates with marketplaces through a unified APISIX layer:
    #   GET  /products?marketplace=…&shop_uuid=…        — list products
    #   GET  /order?marketplace=…&shop_uuid=…           — reserved stock / order
    #   POST /update-stock/{product_id}?marketplace=…   — push stock update
    #   POST /update-status/{product_id}?marketplace=…  — activate/deactivate
    #
    # APISIX handles token lifecycle, routing, transformation, normalization
    # and error mapping. The backend only sends unified fields.
    # ------------------------------------------------------------------
    apisix_base_url: str = "http://192.168.1.185:9080"
    apisix_products_path: str = "/products"
    apisix_reserved_stock_path: str = "/order"
    apisix_order_path: str = "/order"
    # Base URL khusus panggilan ORDER (kosong = pakai apisix_base_url).
    apisix_order_base_url: str = ""
    apisix_update_stock_path: str = "/update-stock"
    apisix_update_status_path: str = "/update-status"
    apisix_update_status_timeout: float = 30.0
    apisix_update_stock_timeout: float = 30.0
    # Safety cap pagination sweep (anti infinite-loop).
    apisix_max_pages: int = 100
    # Shop UUID untuk GET /order (kosong = pakai shop dari payload).
    apisix_shopee_shop_uuid: str = ""
    apisix_tiktok_shop_uuid: str = ""
    # Default warehouse_id untuk /update-stock Format B (single warehouse per SKU).
    apisix_shopee_default_warehouse_id: str = "IDZ"
    apisix_tiktok_default_warehouse_id: str = ""

    # ------------------------------------------------------------------
    # API server
    # ------------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_allowed_hosts: List[str] = ["*"]

    # ------------------------------------------------------------------
    # Backend self-call (Camunda external task → backend)
    # ------------------------------------------------------------------
    backend_base_url: str = "http://localhost:8000"
    backend_self_call_timeout: float = 30.0

    # ------------------------------------------------------------------
    # Stock Sync Scheduler
    # ------------------------------------------------------------------
    stock_sync_enabled: bool = True
    stock_sync_auto_enabled: bool = True
    stock_sync_push_enabled: bool = True
    stock_sync_report_only: bool = False
    stock_sync_skip_if_pending: bool = True
    # Retry push FAILED (di akhir cycle).
    stock_sync_retry_enabled: bool = True
    stock_sync_max_retry_count: int = 3
    stock_sync_retry_backoff_seconds: float = 2.0
    stock_sync_retry_error_markers: str = "400"
    # Jadwal harian tetap (HH:MM 24 jam) + timezone IANA.
    stock_sync_schedule_time: str = "09:41"
    stock_sync_schedule_timezone: str = "Asia/Jakarta"
    stock_sync_report_retention_days: int = 7
    stock_sync_log_retention_days: int = 30
    stock_sync_queue_retention_days: int = 7

    # ------------------------------------------------------------------
    # Mapping Validation (TC-SKU-006)
    # ------------------------------------------------------------------
    mapping_block_pack_shared_part: bool = True
    sync_block_pack_sx_conflict: bool = True
    mapping_validation_enabled: bool = True
    validation_issue_retention_days: int = 30

    # ------------------------------------------------------------------
    # Inventory Report formula
    # ------------------------------------------------------------------
    stock_buffer: int = 0
    # [TIDAK DIPAKAI LAGI] faktor pengali dihitung per SKU oleh domain rules.
    # Dibiarkan utk backward-compat env var yang mungkin sudah ada.
    inventree_factor_default: float = 1.0

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    log_level: str = "INFO"
    log_format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    # ------------------------------------------------------------------
    # Camunda: webhook → start event / message
    # ------------------------------------------------------------------
    camunda_enabled: bool = True
    camunda_base_url: str = "http://localhost:8080"
    camunda_version: str = "7"
    camunda_rest_path: str = ""
    camunda_match_mode: str = "all"
    camunda_process_definition_key: str = ""
    camunda_process_definition_id: str = ""
    camunda_message_name: str = ""
    camunda_tenant_id: str = ""
    camunda_business_key_template: str = ""
    camunda_typed_variables: bool = True
    camunda_timeout_seconds: float = 10.0
    camunda_max_attempts: int = 3
    camunda_retry_delay_seconds: float = 1.0
    camunda_retry_on_status: str = "429,500,502,503,504"
    camunda_auth_type: str = "none"
    camunda_username: str = ""
    camunda_password: str = ""
    camunda_token: str = ""
    camunda_verify_ssl: bool = True
    camunda_rules_file: str = "infrastructure/camunda/config.json"
    camunda_log_enabled: bool = True
    camunda_log_dir: str = "logs/camunda"
    camunda_log_filename: str = "camunda_trigger.log"

    # ------------------------------------------------------------------
    # Webhook receiver (marketplace webhooks from APISIX)
    # ------------------------------------------------------------------
    webhook_log_enabled: bool = True
    webhook_log_dir: str = "logs/webhook"
    webhook_log_filename: str = "order_webhook.log"
    webhook_log_max_body_chars: int = 20000
    # Lokasi field kode signal per marketplace (shopee="code", tiktok="type").
    signal_code_paths: Dict[str, str] = {"shopee": "code", "tiktok": "type"}

    # Webhook → detail order via APISIX
    webhook_order_detail_enabled: bool = True
    webhook_order_detail_translate: bool = False
    webhook_order_detail_timeout: float = 30.0
    # Webhook → part_id (resolusi produk pesanan)
    webhook_part_id_enabled: bool = True

    # Webhook → total reserved stock per produk
    webhook_reserved_enabled: bool = True
    webhook_reserved_max_products: int = 20
    webhook_reserved_retry_delay: float = 3.0

    # Gate status order saat start event Camunda (dipisah koma, kosong = tanpa gate).
    webhook_order_status_allowlist: str = ""


settings = Settings()
