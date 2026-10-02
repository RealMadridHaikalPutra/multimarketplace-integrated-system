import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from infrastructure.db.base import Base


class MarketplaceModel(Base):
    __tablename__ = "marketplace"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    shops = relationship(
        "MarketplaceShopModel", back_populates="marketplace", lazy="selectin"
    )


class MarketplaceShopModel(Base):
    __tablename__ = "marketplace_shop"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    marketplace_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("marketplace.id"), nullable=False
    )
    external_shop_id: Mapped[str] = mapped_column(
        String(100), nullable=False, unique=True, index=True
    )
    shop_name: Mapped[str] = mapped_column(String(255), nullable=False)
    default_warehouse_id: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        index=True,
        comment=(
            "Per-shop warehouse used by stock_sync pipeline → APISIX "
            "/update-stock Format A `skus[].inventory[].warehouse_id`. NULL "
            "falls back to global config (apisix_*_default_warehouse_id)."
        ),
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    marketplace = relationship("MarketplaceModel", back_populates="shops")
    products = relationship(
        "MarketplaceProductModel", back_populates="shop", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint(
            "marketplace_id", "external_shop_id", name="uq_marketplace_shop_external"
        ),
    )


class MarketplaceProductModel(Base):
    __tablename__ = "marketplace_product"

    marketplace_product_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    shop_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("marketplace_shop.external_shop_id"),
        nullable=False,
        index=True,
    )
    external_product_id: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True
    )
    product_name: Mapped[str] = mapped_column(String(500), nullable=False)
    product_image: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")

    shop = relationship("MarketplaceShopModel", back_populates="products")
    variants = relationship(
        "MarketplaceVariantModel", back_populates="product", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint(
            "shop_id", "external_product_id", name="uq_marketplace_product_external"
        ),
    )


class MarketplaceVariantModel(Base):
    __tablename__ = "marketplace_variant"

    marketplace_variant_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    marketplace_product_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("marketplace_product.marketplace_product_id"),
        nullable=False,
        index=True,
    )
    external_variant_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )
    variant_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    seller_sku: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    total_reserved_stock: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_available_stock: Mapped[int | None] = mapped_column(Integer, nullable=True)
    location_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")

    product = relationship("MarketplaceProductModel", back_populates="variants")
    sku_mappings = relationship(
        "SKUMappingModel", back_populates="marketplace_variant", lazy="selectin"
    )
    stock_sync_queues = relationship(
        "StockSyncQueueModel", back_populates="marketplace_variant", lazy="selectin"
    )
    stock_sync_logs = relationship(
        "StockSyncLogModel", back_populates="marketplace_variant", lazy="selectin"
    )

    __table_args__ = (
        UniqueConstraint(
            "marketplace_product_id",
            "external_variant_id",
            name="uq_marketplace_variant_external",
        ),
    )


class SKUMappingModel(Base):
    __tablename__ = "sku_mapping"

    sku_mapping_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    marketplace_variant_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("marketplace_variant.marketplace_variant_id"),
        nullable=False,
        index=True,
    )
    inventree_part_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sku_type: Mapped[str] = mapped_column(String(20), nullable=False, default="NORMAL")
    # faktor pengali: SKU_X=1/qty, SKU_S=1, SKU_PACK=qty, NORMAL=1.
    conversion_qty: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    marketplace_variant = relationship(
        "MarketplaceVariantModel", back_populates="sku_mappings"
    )


class InventoryReportModel(Base):
    """Inventory report combining sku_mapping + stock data (kept for compat)."""

    __tablename__ = "inventory_report"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    part_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    seller_sku: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    sku_type: Mapped[str] = mapped_column(String(20), nullable=False, default="NORMAL")
    stok_inventree: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    buffer: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    faktor_pengali: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    reserved_mp: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved_inventree: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    qty_converted: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    stok_di_update: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    shop_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    marketplace_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class StockSyncQueueModel(Base):
    __tablename__ = "stock_sync_queue"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    marketplace_variant_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("marketplace_variant.marketplace_variant_id"),
        nullable=False,
        index=True,
    )
    target_stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    trigger_source: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    marketplace_variant = relationship(
        "MarketplaceVariantModel", back_populates="stock_sync_queues"
    )


class ValidationIssueModel(Base):
    """Dynamic error/warning tracking table (natural key dedupe)."""

    __tablename__ = "validation_issue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    check_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="WARNING")
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="OPEN", index=True
    )
    entity_type: Mapped[str] = mapped_column(String(30), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(100), nullable=False)
    reference: Mapped[str | None] = mapped_column(String(255), nullable=True)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    action_taken: Mapped[str | None] = mapped_column(String(255), nullable=True)
    suggested_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    marketplace_code: Mapped[str | None] = mapped_column(
        String(50), nullable=True, index=True
    )
    shop_id: Mapped[str | None] = mapped_column(
        String(100), nullable=True, index=True
    )
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "check_code",
            "entity_type",
            "entity_id",
            name="uq_validation_issue_natural_key",
        ),
    )


class StockSyncLogModel(Base):
    __tablename__ = "stock_sync_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    marketplace_variant_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("marketplace_variant.marketplace_variant_id"),
        nullable=False,
        index=True,
    )
    inventree_part_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reference_no: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    previous_stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    target_stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    final_stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="SUCCESS")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    marketplace_variant = relationship(
        "MarketplaceVariantModel", back_populates="stock_sync_logs"
    )


class WebhookLogModel(Base):
    """Log penerimaan dan pemrosesan webhook marketplace.

    Mencatat siklus hidup webhook:
      - Payload mentah dari APISIX
      - Detail pesanan (order_detail dari APISIX)
      - Resolusi part_id dari sku_mapping
      - Status pemrosesan: PENDING -> PROCESSED -> SUCCESS / ON_HOLD
      - ID service task Camunda yang mengeksekusi
    """

    __tablename__ = "webhook_log"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )
    marketplace: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    signal_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    order_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    order_status: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    raw_payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    order_detail: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    part_ids: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="PENDING", index=True
    )
    service_task_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

