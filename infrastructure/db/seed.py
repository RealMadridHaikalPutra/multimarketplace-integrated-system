"""Initial seed data for reference tables.

Auto-executed on application startup via ``init_db()``.
Only inserts data if the tables are empty.
"""

from __future__ import annotations

import logging

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from infrastructure.db.models import MarketplaceModel

logger = logging.getLogger(__name__)


SEED_MARKETPLACES = [
    {"code": "shopee", "name": "Shopee"},
    {"code": "tiktok", "name": "TikTok Shop"},
]


async def seed_marketplaces(session: AsyncSession) -> None:
    """Insert seed marketplace records if the table is empty."""
    result = await session.execute(select(func.count()).select_from(MarketplaceModel))
    count = result.scalar()

    if count and count > 0:
        logger.debug(
            "Marketplace table already has %d records — skipping seed", count
        )
        return

    for data in SEED_MARKETPLACES:
        existing = await session.execute(
            select(MarketplaceModel).where(MarketplaceModel.code == data["code"])
        )
        if not existing.scalar_one_or_none():
            session.add(
                MarketplaceModel(code=data["code"], name=data["name"], status="ACTIVE")
            )

    await session.flush()
    logger.info("Seeded %d marketplace records", len(SEED_MARKETPLACES))


async def run_all_seeds(session: AsyncSession) -> None:
    """Run all seed functions in dependency order."""
    await seed_marketplaces(session)
    logger.info("All seed data initialized")
