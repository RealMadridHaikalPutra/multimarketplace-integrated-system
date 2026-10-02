import uuid
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from domain.entities.marketplace import Marketplace
from domain.enums import MarketplaceStatus
from domain.repositories.marketplace_repository import MarketplaceRepository
from infrastructure.db.models import MarketplaceModel


class MarketplaceRepositoryImpl(MarketplaceRepository):
    def __init__(self, session: AsyncSession):
        self._session = session

    async def create(self, marketplace: Marketplace) -> Marketplace:
        model = MarketplaceModel(
            code=marketplace.code,
            name=marketplace.name,
            status=marketplace.status.value,
        )
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    async def get_by_id(self, marketplace_id: uuid.UUID) -> Optional[Marketplace]:
        result = await self._session.execute(
            select(MarketplaceModel).where(MarketplaceModel.id == marketplace_id)
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def get_by_code(self, code: str) -> Optional[Marketplace]:
        result = await self._session.execute(
            select(MarketplaceModel).where(MarketplaceModel.code == code)
        )
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def list_all(
        self, status: Optional[MarketplaceStatus] = None
    ) -> List[Marketplace]:
        query = select(MarketplaceModel)
        if status:
            query = query.where(MarketplaceModel.status == status.value)
        result = await self._session.execute(query)
        return [self._to_entity(m) for m in result.scalars().all()]

    async def update(self, marketplace: Marketplace) -> Marketplace:
        result = await self._session.execute(
            select(MarketplaceModel).where(MarketplaceModel.id == marketplace.id)
        )
        model = result.scalar_one_or_none()
        if not model:
            raise ValueError(f"Marketplace with id {marketplace.id} not found")

        model.code = marketplace.code
        model.name = marketplace.name
        model.status = marketplace.status.value
        await self._session.flush()
        await self._session.refresh(model)
        return self._to_entity(model)

    @staticmethod
    def _to_entity(model: MarketplaceModel) -> Marketplace:
        return Marketplace(
            id=model.id,
            code=model.code,
            name=model.name,
            status=MarketplaceStatus(model.status),
            created_at=model.created_at,
            updated_at=model.updated_at,
        )
