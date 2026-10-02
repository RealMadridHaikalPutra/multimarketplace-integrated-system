"""
Comprehensive tests for ApisixClient.

Tests cover:
- ApisixClient.update_status() — success, error, config validation, HTTP errors
- ApisixClient.update_stock() — smoke test (regression check)
- Full response logging captured for audit
"""

import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio

from infrastructure.marketplace.apisix.client import (
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    TIKTOK_LISTING_PLATFORMS,
    ApisixClient,
    ApisixConfigError,
    CONFIG_ERROR_PREFIX,
)


logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------
# Helper: create a mock httpx.Response with _request set so
# raise_for_status() does not raise RuntimeError.
# ------------------------------------------------------------------------


def _mock_response(
    status_code: int,
    json_data: dict,
    content_type: str = "application/json",
) -> httpx.Response:
    """
    Return an httpx.Response with a minimal ``_request`` attached.
    Without ``_request``, ``response.raise_for_status()`` raises
    ``RuntimeError('Cannot call raise_for_status as the request instance
    has not been set on this response.')``.
    """
    resp = httpx.Response(
        status_code=status_code,
        content=json.dumps(json_data, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": content_type},
    )
    # Attach a dummy request so raise_for_status() passes for 2xx codes
    resp._request = httpx.Request("POST", "http://mock.test/update-status")
    return resp


def _mock_empty_response(status_code: int = 200) -> httpx.Response:
    """Response with empty body."""
    resp = httpx.Response(status_code=status_code, content=b"")
    resp._request = httpx.Request("POST", "http://mock.test/update-status")
    return resp


def _mock_text_response(status_code: int, text: str) -> httpx.Response:
    """Response with plain-text body."""
    resp = httpx.Response(
        status_code=status_code,
        content=text.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
    )
    resp._request = httpx.Request("POST", "http://mock.test/update-status")
    return resp


def _cycling_page_server(pages: list):
    """side_effect utk httpx AsyncClient.get: menyajikan ``pages`` berulang
    (modulo) — dipakai test TikTok di mana fetch_products kini melakukan
    SEKALI sweep per listing_platforms (2 sweep: TOKOPEDIA dulu,
    TIKTOK_SHOP terakhir), tiap sweep memulai pagination dari halaman 1
    lagi."""
    state = {"i": 0}

    def _next(*args, **kwargs):
        page = pages[state["i"] % len(pages)]
        state["i"] += 1
        return _mock_response(200, page)

    return _next


# ======================================================================
# update_status() — Configuration validation (fail-fast, no HTTP call)
# ======================================================================


@pytest.mark.asyncio
async def test_update_status_invalid_marketplace():
    """
    Given an unsupported marketplace code
    When update_status() is called
    Then ApisixConfigError should be raised WITHOUT any HTTP call.
    """
    client = ApisixClient()
    with pytest.raises(ApisixConfigError, match=r"Unsupported marketplace.*update-status"):
        await client.update_status(
            marketplace="bitcoin",
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_ACTIVE,
        )


@pytest.mark.asyncio
async def test_update_status_empty_product_id():
    """
    Given an empty product_id
    When update_status() is called
    Then ApisixConfigError should be raised WITHOUT any HTTP call.
    """
    client = ApisixClient()
    with pytest.raises(ApisixConfigError, match=r"product_id is empty"):
        await client.update_status(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="",
            status=STATUS_ACTIVE,
        )


@pytest.mark.asyncio
async def test_update_status_invalid_status():
    """
    Given an invalid status value
    When update_status() is called
    Then ApisixConfigError should be raised with clear message.
    """
    client = ApisixClient()
    with pytest.raises(ApisixConfigError, match=r"Invalid status value.*ON_FIRE"):
        await client.update_status(
            marketplace="tiktok",
            shop_uuid="shop123",
            product_id="prod456",
            status="ON_FIRE",
        )


@pytest.mark.asyncio
async def test_update_status_none_marketplace():
    """
    Given marketplace=None
    When update_status() is called
    Then ApisixConfigError should be raised WITHOUT any HTTP call.
    """
    client = ApisixClient()
    with pytest.raises(ApisixConfigError, match=r"Unsupported marketplace.*update-status"):
        await client.update_status(
            marketplace=None,  # type: ignore
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_ACTIVE,
        )


# ======================================================================
# update_status() — Success scenarios (mock APISIX HTTP)
# ======================================================================


@pytest.mark.asyncio
async def test_update_status_success(
    apisix_update_status_success_response,
):
    """
    Given a valid request to Shopee marketplace
    When update_status() is called with STATUS_ACTIVE
    Then it should return the unified success envelope from APISIX.
    """
    client = ApisixClient()

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_response(
            200, apisix_update_status_success_response
        )
        MockClient.return_value = mock_instance

        result = await client.update_status(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_ACTIVE,
        )

        # Verify the response structure
        assert result is not None
        assert isinstance(result, dict)
        assert result.get("marketplace") == "shopee"
        assert result.get("action") == "update_status"
        assert result.get("status") == "ACTIVE"
        assert "raw_response" in result
        assert "error" not in result  # This is a success, not error envelope            # Verify the HTTP call was made correctly
        mock_instance.post.assert_called_once()
        call_args, call_kwargs = mock_instance.post.call_args
        assert call_kwargs["json"] == {"status": "ACTIVE"}
        # The URL is in the positional args[0]
        assert "update-status/prod456" in str(call_args[0]), (
            f"URL should contain '/update-status/prod456', got {call_args[0]}"
        )


@pytest.mark.asyncio
async def test_update_status_success_tiktok():
    """
    Given a valid request to TikTok marketplace
    When update_status() is called
    Then it should return the unified success envelope.
    """
    client = ApisixClient()
    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_response(
            200,
            {
                "marketplace": "tiktok",
                "action": "update_status",
                "status": "ACTIVE",
                "raw_response": {
                    "code": 0,
                    "data": {"errors": []},
                    "message": "Success",
                    "request_id": "test-req-id-005",
                },
            },
        )
        MockClient.return_value = mock_instance

        result = await client.update_status(
            marketplace="tiktok",
            shop_uuid="shop-tiktok-001",
            product_id="tiktok-prod-789",
            status=STATUS_ACTIVE,
        )

        assert result["marketplace"] == "tiktok"
        assert result["action"] == "update_status"
        assert result["status"] == "ACTIVE"


@pytest.mark.asyncio
async def test_update_status_empty_2xx_body():
    """
    Given APISIX returns HTTP 200 with empty body
    When update_status() is called
    Then it should return neutral success dict (no parse error).
    """
    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_empty_response(200)
        MockClient.return_value = mock_instance

        client = ApisixClient()
        result = await client.update_status(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_ACTIVE,
        )

        assert result == {"status": "SUCCESS"}


@pytest.mark.asyncio
async def test_update_status_non_json_2xx_body():
    """
    Given APISIX returns HTTP 200 with non-JSON body
    When update_status() is called
    Then it should return neutral success with raw_body (no crash).
    """
    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_text_response(200, "OK")
        MockClient.return_value = mock_instance

        client = ApisixClient()
        result = await client.update_status(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_INACTIVE,
        )

        assert result.get("status") == "SUCCESS"
        assert "raw_body" in result


# ======================================================================
# update_status() — Error scenarios
# ======================================================================


@pytest.mark.asyncio
async def test_update_status_error_envelope(
    apisix_update_status_error_response,
):
    """
    Given APISIX returns HTTP 200 with error envelope
    When update_status() is called
    Then it should return the error envelope as-is (caller handles).
    """
    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_response(
            200, apisix_update_status_error_response
        )
        MockClient.return_value = mock_instance

        client = ApisixClient()
        result = await client.update_status(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="prod456",
            status=STATUS_ACTIVE,
        )

        assert "error" in result
        assert result["error"]["code"] == "UPSTREAM_ERROR"


@pytest.mark.asyncio
async def test_update_status_http_500():
    """
    Given APISIX returns HTTP 500
    When update_status() is called
    Then httpx.HTTPStatusError should be raised.
    """
    # Create a proper response that raise_for_status() will throw on
    resp = _mock_text_response(500, "Internal Server Error")
    # Override with HTTP 500 status — raise_for_status() checks 4xx/5xx after
    # verifying _request is set, so the RuntimeError is avoided.
    resp = httpx.Response(
        status_code=500,
        content=b"Internal Server Error",
        headers={"Content-Type": "text/plain"},
    )
    resp._request = httpx.Request("POST", "http://mock.test/update-status")

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = resp
        MockClient.return_value = mock_instance

        client = ApisixClient()
        with pytest.raises(httpx.HTTPStatusError):
            await client.update_status(
                marketplace="shopee",
                shop_uuid="shop123",
                product_id="prod456",
                status=STATUS_ACTIVE,
            )


# ======================================================================
# fetch_reserved_stock() — GET /order (reserved stock source of truth,
# TikTok & Shopee, di-match per variant_id)
# ======================================================================


@pytest.mark.asyncio
async def test_fetch_reserved_stock_success():
    """
    Given APISIX /order returns a reserved_stock payload
    When fetch_reserved_stock() is called
    Then it should return the raw JSON dict and hit the /order endpoint
    with the right marketplace & shop_uuid params.
    """
    payload = {
        "marketplace": "tiktok",
        "reserved_stock": [
            {
                "product_id": "1736672569117213740",
                "variants": [
                    {
                        "seller_sku": "8F14-Pack-100",
                        "variant_id": "1736672772166353964",
                        "total_reserved_stock": 3,
                        "orders": [
                            {"status": "AWAITING_SHIPMENT", "order_id": "585383214944388901", "reserved_stock": 1}
                        ],
                    }
                ],
            }
        ],
    }
    client = ApisixClient()

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.return_value = _mock_response(200, payload)
        MockClient.return_value = mock_instance

        result = await client.fetch_reserved_stock(
            marketplace="tiktok",
            shop_uuid="7494709429666874412",
        )

        assert result is not None
        assert result["marketplace"] == "tiktok"
        reserved_variant = result["reserved_stock"][0]["variants"][0]
        assert reserved_variant["variant_id"] == "1736672772166353964"
        assert reserved_variant["total_reserved_stock"] == 3

        call_args, call_kwargs = mock_instance.get.call_args
        assert "/order" in str(call_args[0]), (
            f"URL should contain '/order', got {call_args[0]}"
        )
        assert call_kwargs["params"] == {
            "marketplace": "tiktok",
            "shop_uuid": "7494709429666874412",
        }


@pytest.mark.asyncio
async def test_fetch_reserved_stock_http_500_returns_none():
    """
    Given APISIX /order returns HTTP 500
    When fetch_reserved_stock() is called
    Then it should return None (graceful fallback — sync continues with the
    product-payload reserved stock instead of failing).
    """
    resp = httpx.Response(
        status_code=500,
        content=b"Internal Server Error",
        headers={"Content-Type": "text/plain"},
    )
    resp._request = httpx.Request("GET", "http://mock.test/order")

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.return_value = resp
        MockClient.return_value = mock_instance

        client = ApisixClient()
        result = await client.fetch_reserved_stock(
            marketplace="tiktok",
            shop_uuid="7494709429666874412",
        )

        assert result is None


# ======================================================================
# fetch_products() — pagination sweep (SEMUA halaman, jangan ada yang terlewat)
# ======================================================================


@pytest.mark.asyncio
async def test_fetch_products_sweeps_all_pages_shopee_page_based():
    """
    Shopee (APISIX gateway): SETIAP request membawa ``page`` eksplisit sejak
    halaman 1 (page=1, page=2, …) — parameter-mapping.lua gateway menghitung
    offset dari page; param ``offset`` mentah dari client diabaikan → dulu
    Shopee hanya terambil halaman pertama.

    Response produk Shopee men-echo ``next_page_token`` yang sebenarnya offset
    (mis. "50") dan BERULANG di tiap halaman — token ini HARUS diabaikan
    (Shopee tidak memakai cursor seperti TikTok); sweep lanjut via increment
    ``page`` sampai ``has_next=false``.
    """
    client = ApisixClient()
    page1 = {
        "products": [
            {"external_product_id": "1001", "product_name": "P1"},
            {"external_product_id": "1002", "product_name": "P2"},
        ],
        "pagination": {
            "total": 3, "page": 1, "page_size": 50,
            "has_next": True, "next_page_token": "50",
        },
    }
    page2 = {
        "products": [
            {"external_product_id": "1003", "product_name": "P3"},
        ],
        "pagination": {
            "total": 3, "page": 2, "page_size": 50,
            "has_next": True, "next_page_token": "50",
        },
    }
    page3 = {
        "products": [],
        "pagination": {"total": 3, "page": 3, "page_size": 50, "has_next": False},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, page2),
            _mock_response(200, page3),
        ]
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="shopee", shop_uuid="227674818")

        # Sweep harus menembus token echo "50" yang berulang → sampai has_next=false
        assert mock_instance.get.call_count == 3
        for call_no, expected_page in ((0, 1), (1, 2), (2, 3)):
            params = mock_instance.get.call_args_list[call_no].kwargs["params"]
            assert params == {
                "marketplace": "shopee",
                "shop_uuid": "227674818",
                "page": expected_page,
            }
            assert "next_page_token" not in params  # token response Shopee diabaikan

        assert result["marketplace"] == "shopee"
        assert [p["external_product_id"] for p in result["products"]] == [
            "1001", "1002", "1003",
        ]
        assert result["pages_fetched"] == 3
        assert result["sweep_complete"] is True


@pytest.mark.asyncio
async def test_fetch_products_sweeps_all_pages_cursor_based():
    """
    TikTok (cursor-based): pagination.next_page_token dipakai sebagai param
    ``next_page_token`` pada request berikutnya (nama param sesuai
    parameter-mapping.lua gateway). Param ``page`` TIDAK dikirim.

    TikTok kini di-sweep SEKALI PER listing_platforms (2 sweep: TOKOPEDIA,
    TIKTOK_SHOP) — tiap sweep memulai pagination dari halaman 1 (cursor
    baru), hasil di-merge dedup per product id.
    """
    client = ApisixClient()
    page1 = {
        "products": [{"id": "a1", "title": "A1"}],
        "pagination": {"has_next": True, "next_page_token": "tok-1"},
    }
    page2 = {
        "products": [{"id": "a2", "title": "A2"}],
        "pagination": {"has_next": True, "next_page_token": "tok-2"},
    }
    page3 = {
        "products": [{"id": "a3", "title": "A3"}],
        "pagination": {"has_next": False, "next_page_token": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = _cycling_page_server([page1, page2, page3])
        MockClient.return_value = mock_instance

        result = await client.fetch_products(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        # 2 sweep × 3 halaman = 6 request GET.
        assert mock_instance.get.call_count == 6
        all_calls = mock_instance.get.call_args_list

        # Setiap sweep membawa listing_platforms BERBEDA, urut sesuai konstanta.
        assert all_calls[0].kwargs["params"]["listing_platforms"] == "TOKOPEDIA"
        assert all_calls[3].kwargs["params"]["listing_platforms"] == "TIKTOK_SHOP"

        # Cursor pagination: request ke-2 & ke-3 sweep pertama memakai token.
        first_params = all_calls[0].kwargs["params"]
        assert "page" not in first_params and "next_page_token" not in first_params
        assert all_calls[1].kwargs["params"]["next_page_token"] == "tok-1"
        assert all_calls[2].kwargs["params"]["next_page_token"] == "tok-2"

        # Merge dedup lintas sweep → 3 product unik (a1..a3 dari sweep pertama;
        # sweep kedua mengembalikan product sama → di-dedup).
        assert len(result["products"]) == 3
        assert result["pages_fetched"] == 6
        assert result["sweep_complete"] is True
        assert result["listing_platforms_swept"] == TIKTOK_LISTING_PLATFORMS


@pytest.mark.asyncio
async def test_fetch_products_single_page_without_pagination():
    """
    Response tanpa metadata ``pagination`` (format legacy) → hanya 1 request,
    perilaku identik dengan versi lama (tidak ada yang rusak).
    """
    client = ApisixClient()
    payload = {
        "products": [{"external_product_id": "9001", "product_name": "Legacy"}],
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.return_value = _mock_response(200, payload)
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="shopee", shop_uuid="s1")

        assert mock_instance.get.call_count == 1
        assert len(result["products"]) == 1


@pytest.mark.asyncio
async def test_fetch_products_dedupes_repeated_products():
    """
    Produk yang sama muncul di 2 halaman → hanya disimpan sekali (dedup by id),
    sehingga tidak diproses dua kali oleh SyncEngine.
    """
    client = ApisixClient()
    page1 = {
        "products": [{"id": "111", "title": "Dupe"}],
        "pagination": {"has_next": True, "next_page_token": "tok-1"},
    }
    page2 = {
        "products": [
            {"id": "111", "title": "Dupe"},
            {"id": "222", "title": "Baru"},
        ],
        "pagination": {"has_next": False, "next_page_token": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = _cycling_page_server([page1, page2])
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="tiktok", shop_uuid="s2")

        # 2 sweep × 2 halaman = 4 request; product 111 & 222 sudah terambil di
        # sweep pertama → sweep berikutnya di-dedup.
        assert mock_instance.get.call_count == 4
        assert len(result["products"]) == 2


@pytest.mark.asyncio
async def test_fetch_products_stops_on_repeated_page_token():
    """
    Anti infinite-loop: APISIX mengembalikan ``next_page_token`` yang SAMA
    berulang-ulang → sweep berhenti (tidak looping tanpa batas).
    """
    client = ApisixClient()
    page = {
        "products": [{"id": "x1", "title": "X"}],
        "pagination": {"has_next": True, "next_page_token": "same-token"},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = _cycling_page_server([page, page])
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="tiktok", shop_uuid="s3")

        # Setiap sweep berhenti setelah token berulang (2 request per sweep),
        # bukan infinite loop → 2 sweep × 2 request = 4.
        assert mock_instance.get.call_count == 4
        # Kedua halaman berisi produk yang sama → dedup menyisakan 1.
        assert len(result["products"]) == 1
        # Sweep tidak lengkap (data kemungkinan masih ada) → ditandai eksplisit
        assert result["sweep_complete"] is False


@pytest.mark.asyncio
async def test_fetch_products_cursor_trailing_empty_page_is_complete():
    """
    Kasus NYATA gateway TikTok: halaman terakhir yang BERISI data men-echo
    ``has_next=true`` + ``next_page_token``, lalu halaman berikutnya KOSONG
    dengan ``has_next=true`` TANPA ``next_page_token``.

    Sweep seperti ini BUKAN data terpotong → ``sweep_complete`` tetap True.
    Kritis: SyncEngine memakai flag ini sebagai gate FREEZE — saat flag False,
    product yang sudah tidak listing di TikTok Shop (hanya ada di sweep
    TOKOPEDIA) TIDAK PERNAH di-FREEZE.
    """
    client = ApisixClient()

    def _pages(items, total):
        return [
            {
                "products": items,
                "pagination": {
                    "page": 1, "page_size": 50, "total": total,
                    "has_next": True, "next_page_token": "cursor-1",
                },
            },
            {
                "products": [],
                "pagination": {
                    "page": 2, "page_size": 50, "total": total,
                    "has_next": True, "next_page_token": None,
                },
            },
        ]

    sweep_tokopedia = _pages([{"id": "tp-1"}, {"id": "both-1"}], total=2)
    sweep_tiktok = _pages([{"id": "both-1"}], total=1)

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        counters = {"TOKOPEDIA": 0, "TIKTOK_SHOP": 0}

        def _by_platform(*args, **kwargs):
            platform = (kwargs.get("params") or {}).get("listing_platforms", "")
            pages = sweep_tiktok if platform == "TIKTOK_SHOP" else sweep_tokopedia
            idx = min(counters[platform], len(pages) - 1)
            counters[platform] += 1
            return _mock_response(200, pages[idx])

        mock_instance.get.side_effect = _by_platform
        MockClient.return_value = mock_instance

        result = await client.fetch_products(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        # 2 sweep × 2 halaman (termasuk halaman ekor kosong) = 4 request.
        assert mock_instance.get.call_count == 4
        assert result["sweep_complete"] is True
        assert [p["id"] for p in result["products"]] == ["tp-1", "both-1"]
        # tp-1 hanya di TOKOPEDIA → bahan FREEZE di SyncEngine.
        assert result["product_listing_platforms"] == {
            "tp-1": ["TOKOPEDIA"],
            "both-1": ["TOKOPEDIA", "TIKTOK_SHOP"],
        }


@pytest.mark.asyncio
async def test_fetch_products_cursor_missing_token_with_new_items_still_incomplete():
    """
    Anti false-positive: halaman yang MASIH menambah product baru tapi tanpa
    ``next_page_token`` → data kemungkinan masih ada & tak bisa dilanjutkan →
    sweep tetap ditandai TIDAK lengkap (FREEZE/cleanup tidak dijalankan).
    """
    client = ApisixClient()
    page1 = {
        "products": [{"id": "b1"}],
        "pagination": {
            "has_next": True, "next_page_token": "c1", "total": 10,
        },
    }
    page2 = {
        "products": [{"id": "b2"}],
        "pagination": {
            "has_next": True, "next_page_token": None, "total": 10,
        },
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = _cycling_page_server([page1, page2])
        MockClient.return_value = mock_instance

        result = await client.fetch_products(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        assert result["sweep_complete"] is False


@pytest.mark.asyncio
async def test_fetch_products_cursor_complete_when_total_reached():
    """
    Bila seluruh ``pagination.total`` sudah terambil, sweep dianggap TUNTAS
    walau halaman terakhir masih men-echo ``has_next=true`` tanpa token.
    """
    client = ApisixClient()
    page = {
        "products": [{"id": "t1"}, {"id": "t2"}],
        "pagination": {
            "has_next": True, "next_page_token": None, "total": 2,
        },
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.return_value = _mock_response(200, page)
        MockClient.return_value = mock_instance

        result = await client.fetch_products(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        assert result["sweep_complete"] is True
        assert len(result["products"]) == 2


@pytest.mark.asyncio
async def test_fetch_products_max_pages_safety_cap():
    """
    Safety cap ``settings.apisix_max_pages`` menghentikan sweep page-based
    Shopee yang tidak pernah selesai (has_next=True terus-menerus, tiap halaman
    selalu memberi produk baru), supaya tidak looping selamanya. Sweep ditandai
    tidak lengkap.
    """
    client = ApisixClient()

    page_counter = {"n": 0}

    def _next_page(*args, **kwargs):
        page_counter["n"] += 1
        return _mock_response(
            200,
            {
                "products": [
                    {
                        "external_product_id": f"cap-{page_counter['n']}-{n}",
                        "product_name": "C",
                    }
                    for n in range(5)
                ],
                "pagination": {
                    "has_next_page": True,
                    "next_offset": 10,
                },
            },
        )

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = _next_page
        MockClient.return_value = mock_instance

        with patch("core.config.settings.apisix_max_pages", 3):
            result = await client.fetch_products(marketplace="shopee", shop_uuid="s4")

        # Loop berhenti tepat di cap (3 request), bukan tak terbatas
        assert mock_instance.get.call_count == 3
        assert result["pages_fetched"] == 3
        assert result["sweep_complete"] is False


@pytest.mark.asyncio
async def test_fetch_products_stops_when_shopee_pages_have_no_new_items():
    """
    Shopee page-based: bila halaman berturut-turut TIDAK menambah produk baru
    padahal masih has_next_page (gateway mengabaikan param page / offset macet),
    ada toleransi 1 halaman retry, lalu sweep berhenti & ditandai TIDAK lengkap
    (tidak looping sampai safety cap).
    """
    client = ApisixClient()
    page = {
        "products": [{"external_product_id": "1001", "product_name": "P1"}],
        "pagination": {"has_next_page": True, "next_offset": 10},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [
            _mock_response(200, page),
            _mock_response(200, page),
            _mock_response(200, page),
        ]
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="shopee", shop_uuid="s5")

        # page1 isi, page2 & page3 duplikat → berhenti setelah 2 halaman macet
        assert mock_instance.get.call_count == 3
        assert len(result["products"]) == 1
        assert result["sweep_complete"] is False


@pytest.mark.asyncio
async def test_fetch_products_shopee_recovers_after_one_empty_page():
    """
    Shopee offset pagination dapat punya halaman tengah kosong (offset melewati
    item terfilter). Satu halaman kosong tidak boleh langsung menghentikan sweep:
    halaman berikutnya yang berisi produk baru tetap dilanjutkan sampai tuntas.
    """
    client = ApisixClient()
    page1 = {
        "products": [{"external_product_id": "1001", "product_name": "P1"}],
        "pagination": {"has_next_page": True, "next_offset": 10},
    }
    empty_middle = {
        "products": [],
        "pagination": {"has_next_page": True, "next_offset": 20},
    }
    page3 = {
        "products": [{"external_product_id": "1002", "product_name": "P2"}],
        "pagination": {"has_next_page": False, "next_offset": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, empty_middle),
            _mock_response(200, page3),
        ]
        MockClient.return_value = mock_instance

        result = await client.fetch_products(marketplace="shopee", shop_uuid="s6")

        assert mock_instance.get.call_count == 3
        assert len(result["products"]) == 2
        assert result["sweep_complete"] is True


@pytest.mark.asyncio
async def test_fetch_products_http_error_raises():
    """
    Error HTTP di halaman mana pun → tetap raise (product adalah sumber utama
    sync, tidak ada fallback — perilaku lama dipertahankan).
    """
    resp = httpx.Response(
        status_code=500,
        content=b"Internal Server Error",
        headers={"Content-Type": "text/plain"},
    )
    resp._request = httpx.Request("GET", "http://mock.test/products")

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.return_value = resp
        MockClient.return_value = mock_instance

        client = ApisixClient()
        with pytest.raises(httpx.HTTPStatusError):
            await client.fetch_products(marketplace="shopee", shop_uuid="s1")


@pytest.mark.asyncio
async def test_fetch_products_tiktok_merge_dedups_cross_listing_platforms():
    """
    Product yang sama cross-listed TikTok+Tokopedia muncul di sweep
    TOKOPEDIA dan di sweep TIKTOK_SHOP → hanya diproses SEKALI (urutan sweep:
    TOKOPEDIA dulu, TIKTOK_SHOP terakhir). Product yang CUMA ada di
    TIKTOK_SHOP tetap terambil dari sweep-nya sendiri.
    """
    client = ApisixClient()

    sweep_tokopedia = {
        "products": [
            {"id": "tp-1", "title": "Tokopedia only"},
            {"id": "both-1", "title": "Cross-listed"},
        ],
        "pagination": {"has_next": False, "next_page_token": None},
    }
    sweep_tiktok = {
        "products": [
            {"id": "tt-1", "title": "TikTok only"},
            {"id": "both-1", "title": "Cross-listed"},
            {"id": "both-2", "title": "Cross-listed v2"},
        ],
        "pagination": {"has_next": False, "next_page_token": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance

        def _by_platform(*args, **kwargs):
            platform = (kwargs.get("params") or {}).get("listing_platforms", "")
            if platform == "TIKTOK_SHOP":
                return _mock_response(200, sweep_tiktok)
            return _mock_response(200, sweep_tokopedia)

        mock_instance.get.side_effect = _by_platform
        MockClient.return_value = mock_instance

        result = await client.fetch_products(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        product_ids = [p["id"] for p in result["products"]]
        # Semua product unik terambil, cross-listed hanya sekali (urutan sweep:
        # TOKOPEDIA dulu → both-1 from TOKOPEDIA sweep, both-2 from TIKTOK_SHOP).
        assert product_ids == ["tp-1", "both-1", "tt-1", "both-2"]
        assert result["pages_fetched"] == 2  # 1 halaman per sweep
        assert result["sweep_complete"] is True
        assert result["listing_platforms_swept"] == TIKTOK_LISTING_PLATFORMS
        # Peta per-product listing platforms: both-1 ada di KEDUA sweep,
        # tp-1 hanya TOKOPEDIA, tt-1 & both-2 hanya TIKTOK_SHOP.
        assert result["product_listing_platforms"] == {
            "tp-1": ["TOKOPEDIA"],
            "both-1": ["TOKOPEDIA", "TIKTOK_SHOP"],
            "tt-1": ["TIKTOK_SHOP"],
            "both-2": ["TIKTOK_SHOP"],
        }


# ======================================================================
# fetch_reserved_stock() — pagination sweep /order (cursor-based)
# ======================================================================


@pytest.mark.asyncio
async def test_fetch_reserved_stock_sweeps_all_pages():
    """
    /order juga di-sweep — entri ``reserved_stock`` dari semua halaman digabung
    (TikTok cursor-based via next_page_token).
    """
    client = ApisixClient()
    page1 = {
        "marketplace": "tiktok",
        "reserved_stock": [
            {"product_id": "p1", "variants": [{"variant_id": "v1", "total_reserved_stock": 3}]}
        ],
        "pagination": {"has_next": True, "next_page_token": "tok-a"},
    }
    page2 = {
        "marketplace": "tiktok",
        "reserved_stock": [
            {"product_id": "p2", "variants": [{"variant_id": "v2", "total_reserved_stock": 5}]}
        ],
        "pagination": {"has_next": False, "next_page_token": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, page2),
        ]
        MockClient.return_value = mock_instance

        result = await client.fetch_reserved_stock(
            marketplace="tiktok", shop_uuid="7494709429666874412"
        )

        assert result is not None
        assert mock_instance.get.call_count == 2
        second_params = mock_instance.get.call_args_list[1].kwargs["params"]
        assert second_params["next_page_token"] == "tok-a"

        variants = []
        for entry in result["reserved_stock"]:
            variants.extend(entry["variants"])
        assert {v["variant_id"]: v["total_reserved_stock"] for v in variants} == {
            "v1": 3,
            "v2": 5,
        }


@pytest.mark.asyncio
async def test_fetch_reserved_stock_sweeps_shopee_page_based():
    """
    /order Shopee juga di-sweep page-based: metadata has_next_page hanya
    penanda has_more — request berikutnya menaikkan ``page`` (gateway
    memetakan page → page_no). Entri antar halaman digabung & di-dedup.
    """
    client = ApisixClient()
    page1 = {
        "marketplace": "shopee",
        "reserved_stock": [
            {"product_id": "p1", "variants": [{"variant_id": "v1", "total_reserved_stock": 3}]}
        ],
        "pagination": {"page": 1, "has_next_page": True, "next_offset": 100},
    }
    page2 = {
        "marketplace": "shopee",
        "reserved_stock": [
            {"product_id": "p2", "variants": [{"variant_id": "v2", "total_reserved_stock": 5}]}
        ],
        "pagination": {"page": 2, "has_next_page": False, "next_offset": None},
    }

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [
            _mock_response(200, page1),
            _mock_response(200, page2),
        ]
        MockClient.return_value = mock_instance

        result = await client.fetch_reserved_stock(
            marketplace="shopee", shop_uuid="227674818"
        )

        assert result is not None
        assert mock_instance.get.call_count == 2
        first_params = mock_instance.get.call_args_list[0].kwargs["params"]
        assert first_params["page"] == 1
        assert "offset" not in first_params
        assert "next_page_token" not in first_params
        second_params = mock_instance.get.call_args_list[1].kwargs["params"]
        assert second_params["page"] == 2
        assert "offset" not in second_params
        assert "next_page_token" not in second_params

        variants = []
        for entry in result["reserved_stock"]:
            variants.extend(entry["variants"])
        assert {v["variant_id"]: v["total_reserved_stock"] for v in variants} == {
            "v1": 3,
            "v2": 5,
        }
        assert result["sweep_complete"] is True


@pytest.mark.asyncio
async def test_fetch_reserved_stock_partial_on_later_page_failure():
    """
    Halaman pertama OK, halaman kedua HTTP 500 → return data PARSIAL dari
    halaman pertama (bukan None), supaya data reserved yang sudah didapat
    tidak hilang seluruhnya.
    """
    client = ApisixClient()
    page1 = {
        "reserved_stock": [
            {"product_id": "p1", "variants": [{"variant_id": "v1", "total_reserved_stock": 3}]}
        ],
        "pagination": {"has_next": True, "next_page_token": "tok-b"},
    }
    error_resp = httpx.Response(
        status_code=500,
        content=b"Internal Server Error",
        headers={"Content-Type": "text/plain"},
    )
    error_resp._request = httpx.Request("GET", "http://mock.test/order")

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.get.side_effect = [_mock_response(200, page1), error_resp]
        MockClient.return_value = mock_instance

        result = await client.fetch_reserved_stock(marketplace="tiktok", shop_uuid="s1")

        assert result is not None
        assert len(result["reserved_stock"]) == 1
        assert result["reserved_stock"][0]["product_id"] == "p1"
        assert result["sweep_complete"] is False


# ======================================================================
# update_stock() — Smoke test (regression check)
# ======================================================================


@pytest.mark.asyncio
async def test_update_stock_success(
    apisix_update_stock_success_response,
):
    """
    Given a valid stock update request
    When update_stock() is called
    Then it should return the unified success envelope.
    """
    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = _mock_response(
            200, apisix_update_stock_success_response
        )
        MockClient.return_value = mock_instance

        client = ApisixClient()
        result = await client.update_stock(
            marketplace="shopee",
            shop_uuid="shop123",
            product_id="prod456",
            variant_id="variant001",
            stock=150,
            warehouse_id="WH-001",
        )

        assert result["marketplace"] == "shopee"
        assert "raw_response" in result
        assert "error" not in result

        # Verify format B body
        call_json = mock_instance.post.call_args.kwargs["json"]
        assert call_json["skus"][0]["id"] == "variant001"
        assert call_json["skus"][0]["stock"] == 150
        assert call_json["skus"][0]["warehouse_id"] == "WH-001"


# ======================================================================
# HTTP error response body — embedded into exception message (audit trail)
# ======================================================================


def _http_error_response(status_code: int, body: str) -> httpx.Response:
    """HTTP error response dengan body JSON (APISIX error envelope)."""
    resp = httpx.Response(
        status_code=status_code,
        content=body.encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    resp._request = httpx.Request(
        "POST", "http://mock.test/update-stock/prod456"
    )
    return resp


@pytest.mark.asyncio
@pytest.mark.parametrize("api_name,kwargs", [
    ("update_stock", dict(
        marketplace="shopee", shop_uuid="shop123", product_id="prod456",
        variant_id="variant001", stock=10, warehouse_id="WH-001",
    )),
    ("update_stock_batch", dict(
        marketplace="shopee", shop_uuid="shop123", product_id="prod456",
        skus=[{"id": "variant001", "quantity": 10}], warehouse_id="WH-001",
    )),
    ("update_status", dict(
        marketplace="shopee", shop_uuid="shop123", product_id="prod456",
        status=STATUS_ACTIVE,
    )),
])
async def test_http_error_message_contains_full_response_body(api_name, kwargs):
    """
    APISIX/marketplace mengembalikan HTTP 500 dengan JSON error envelope.

    Exception yang di-raise HARUS memuat FULL response body di message-nya
    (bukan hanya summary line 'Server error ... for url') supaya caller yang
    hanya menyimpan str(exc) ke stock_sync_queue.error_message /
    stock_sync_log.error_message tetap punya detail respons lengkap.
    """
    error_body = json.dumps({
        "error": {
            "code": "UPSTREAM_ERROR",
            "message": "model not found in shop",
            "request_id": "req-xyz-999",
            "api_error": "shopee.model.not_found",
        }
    })
    resp = _http_error_response(500, error_body)

    with patch.object(httpx, "AsyncClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock_instance.post.return_value = resp
        MockClient.return_value = mock_instance

        client = ApisixClient()
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await getattr(client, api_name)(**kwargs)

    msg = str(exc_info.value)
    # Summary line tetap ada...
    assert "Server error '500" in msg
    # ...dan FULL response body juga tersimpan di message:
    assert "UPSTREAM_ERROR" in msg
    assert "model not found in shop" in msg
    assert "req-xyz-999" in msg
    assert "shopee.model.not_found" in msg
    assert "HTTP 500 response body:" in msg

    # Struktur exception tetap utuh (request & response tetap ter-attach).
    assert exc_info.value.response.status_code == 500
    assert exc_info.value.request is not None



