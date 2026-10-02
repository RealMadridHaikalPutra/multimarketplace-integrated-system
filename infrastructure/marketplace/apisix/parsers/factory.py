import logging
from typing import Optional

from infrastructure.marketplace.apisix.parsers.base import MarketplaceParser
from infrastructure.marketplace.apisix.parsers.shopee import ShopeeParser
from infrastructure.marketplace.apisix.parsers.tiktok import TikTokParser

logger = logging.getLogger(__name__)

_PARSER_REGISTRY = {
    "shopee": ShopeeParser,
    "shopeesandbox": ShopeeParser,
    "tiktok": TikTokParser,
    "tiktoksandbox": TikTokParser,
    "tokopedia": ShopeeParser,  # fallback — adjust if Tokopedia format differs
}


def get_parser(marketplace: str) -> Optional[MarketplaceParser]:
    """Return the parser for a marketplace code, or None when unregistered."""
    parser_cls = _PARSER_REGISTRY.get(marketplace.lower())
    if not parser_cls:
        logger.error("No parser registered for marketplace '%s'", marketplace)
        return None

    logger.info("Using parser '%s' for marketplace '%s'", parser_cls.__name__, marketplace)
    return parser_cls()
