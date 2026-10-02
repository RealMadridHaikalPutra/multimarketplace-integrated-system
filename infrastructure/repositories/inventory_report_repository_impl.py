from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.inventory_report import InventoryReport
from domain.repositories.inventory_report_repository import InventoryReportRepository
from infrastructure.db.models import InventoryReportModel


class InventoryReportRepositoryImpl(InventoryReportRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, report: InventoryReport) -> InventoryReport:
        model = InventoryReportModel(
            part_id=report.part_id,
            seller_sku=report.seller_sku,
            sku_type=report.sku_type,
            stok_inventree=report.stok_inventree,
            buffer=report.buffer,
            faktor_pengali=report.faktor_pengali,
            reserved_mp=report.reserved_mp,
            reserved_inventree=report.reserved_inventree,
            qty_converted=report.qty_converted,
            stok_di_update=report.stok_di_update,
            shop_id=report.shop_id,
            marketplace_code=report.marketplace_code,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def list_for_shop(
        self, shop_id: str, marketplace_code: Optional[str] = None, limit: int = 1000
    ) -> List[InventoryReport]:
        query = select(InventoryReportModel).where(
            InventoryReportModel.shop_id == shop_id
        )
        if marketplace_code:
            query = query.where(
                InventoryReportModel.marketplace_code == marketplace_code
            )
        query = query.order_by(InventoryReportModel.generated_at.desc()).limit(limit)
        result = await self._session.execute(query)
        return [self._to_entity(m) for m in result.scalars().all()]

    async def list_latest_for_seller_sku(
        self, seller_sku: str, limit: int = 1
    ) -> List[InventoryReport]:
        result = await self._session.execute(
            select(InventoryReportModel)
            .where(InventoryReportModel.seller_sku == seller_sku)
            .order_by(InventoryReportModel.generated_at.desc())
            .limit(limit)
        )
        return [self._to_entity(m) for m in result.scalars().all()]

    @staticmethod
    def _to_entity(model: InventoryReportModel) -> InventoryReport:
        return InventoryReport(
            id=model.id,
            part_id=model.part_id,
            seller_sku=model.seller_sku,
            sku_type=model.sku_type,
            stok_inventree=model.stok_inventree,
            buffer=model.buffer,
            faktor_pengali=model.faktor_pengali,
            reserved_mp=model.reserved_mp,
            reserved_inventree=model.reserved_inventree,
            qty_converted=model.qty_converted,
            stok_di_update=model.stok_di_update,
            shop_id=model.shop_id,
            marketplace_code=model.marketplace_code,
            generated_at=model.generated_at,
        )
