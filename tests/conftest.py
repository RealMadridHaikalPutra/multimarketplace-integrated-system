"""
Shared pytest fixtures for the entire test suite.

Provides:
- Mock httpx client for APISIX calls
- Sample product / variant / mapping entities
- In-memory or async test DB session (when needed)
"""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio

from domain.entities.inventory_report import InventoryReport
from domain.entities.marketplace_product import MarketplaceProduct
from domain.entities.marketplace_variant import MarketplaceVariant
from domain.enums import RecordStatus


# ---------------------------------------------------------------------------
# Fixtures: sample data
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_product_inactive() -> MarketplaceProduct:
    """Return a MarketplaceProduct with INACTIVE status."""
    return MarketplaceProduct(
        marketplace_product_id="shop123_prod456",
        shop_id="shop123",
        external_product_id="prod456",
        product_name="Test Product Inactive",
        status=RecordStatus.INACTIVE,
    )


@pytest.fixture
def sample_product_active() -> MarketplaceProduct:
    """Return a MarketplaceProduct with ACTIVE status."""
    return MarketplaceProduct(
        marketplace_product_id="shop123_prod789",
        shop_id="shop123",
        external_product_id="prod789",
        product_name="Test Product Active",
        status=RecordStatus.ACTIVE,
    )


@pytest.fixture
def sample_variant() -> MarketplaceVariant:
    """Return a MarketplaceVariant with seller SKU."""
    return MarketplaceVariant(
        marketplace_variant_id="shop123_variant001",
        shop_id="shop123",
        external_product_id="prod456",
        external_variant_id="variant001",
        variant_name="Variant A",
        seller_sku="TEST-SKU-001",
        total_reserved_stock=5,
        total_available_stock=50,
        location_id="WH-001",
        status=RecordStatus.ACTIVE,
    )


@pytest.fixture
def sample_shop() -> dict:
    """Return a mock shop dict (as returned by repository)."""
    return {
        "id": "shop-uuid-123",
        "marketplace_id": "mp-uuid-001",
        "external_shop_id": "shop123",
        "shop_name": "Test Shop",
        "default_warehouse_id": "WH-001",
        "status": "ACTIVE",
    }


# ---------------------------------------------------------------------------
# Fixtures: mock APISIX responses
# ---------------------------------------------------------------------------


@pytest.fixture
def apisix_update_status_success_response() -> dict:
    """Successful /update-status response per APISIX docs."""
    return {
        "marketplace": "shopee",
        "action": "update_status",
        "status": "ACTIVE",
        "raw_response": {
            "error": "",
            "message": "",
            "request_id": "test-req-id-001",
            "response": {
                "item_id": 802023254,
                "item_status": "NORMAL",
                "success_list": [],
                "failure_list": [],
            },
        },
    }


@pytest.fixture
def apisix_update_status_error_response() -> dict:
    """Error /update-status response per APISIX docs."""
    return {
        "error": {
            "code": "UPSTREAM_ERROR",
            "message": "product.error_unknown: failed to update item status",
            "marketplace": "shopee",
            "original_status": 400,
            "original_code": "product.error_unknown",
            "request_id": "test-req-id-002",
        }
    }


@pytest.fixture
def apisix_update_stock_success_response() -> dict:
    """Successful /update-stock response per APISIX docs."""
    return {
        "marketplace": "shopee",
        "raw_response": {
            "error": "",
            "message": "",
            "request_id": "test-req-id-003",
            "response": {
                "failure_list": [],
                "success_list": [
                    {
                        "model_id": 0,
                        "location_id": "",
                        "stock": 150,
                    }
                ],
            },
        },
    }


@pytest.fixture
def apisix_update_stock_error_response() -> dict:
    """Error /update-stock response per APISIX docs."""
    return {
        "error": {
            "code": "MISSING_PRODUCT_ID",
            "message": "product_id is required (provide in URL path)",
            "marketplace": "shopee",
            "original_status": 400,
            "original_code": None,
            "request_id": "test-req-id-004",
        }
    }


# ---------------------------------------------------------------------------
# Fixtures: mock httpx transport
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_http_transport():
    """
    Returns a factory that creates a mock httpx transport.
    Usage:
        transport = mock_http_transport(response_data)
        async with httpx.AsyncClient(transport=transport) as client:
            ...
    """
    def _create_transport(
        response_data: dict,
        status_code: int = 200,
    ) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            body = json.dumps(response_data).encode("utf-8")
            return httpx.Response(
                status_code=status_code,
                content=body,
                headers={"Content-Type": "application/json"},
            )
        return httpx.MockTransport(handler)

    return _create_transport
