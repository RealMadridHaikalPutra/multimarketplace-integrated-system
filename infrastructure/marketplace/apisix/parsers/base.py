from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from domain.enums import RecordStatus


@dataclass
class ParsedProduct:
    """Normalized product from any marketplace."""

    external_product_id: str
    product_name: str
    product_image: Optional[str]
    variants: Optional[List["ParsedVariant"]] = None
    category: Optional[str] = None
    description: Optional[str] = None
    status: RecordStatus = RecordStatus.ACTIVE


@dataclass
class ParsedVariant:
    """Normalized SKU variant from any marketplace."""

    external_variant_id: Optional[str]
    variant_name: Optional[str]
    seller_sku: str
    stock: Optional[int] = None
    reserved_stock: Optional[int] = None
    available_stock: Optional[int] = None
    location_id: Optional[str] = None
    status: RecordStatus = RecordStatus.ACTIVE


class MarketplaceParser(ABC):
    """Base class for marketplace-specific response parsers."""

    @abstractmethod
    def parse_products(self, raw_data: dict) -> List[ParsedProduct]:
        """Parse a raw marketplace response into normalized products."""
        raise NotImplementedError
