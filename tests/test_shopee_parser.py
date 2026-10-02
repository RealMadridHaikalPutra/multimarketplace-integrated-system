"""
Unit tests for ShopeeParser.

Covers:
- Products WITH models (tier variation) → parsed as variants (model_id).
- Products WITHOUT models (has_model=false, model=[]) → fallback ke 1
  variant sintetis: seller_sku = item_sku, stock dari level item
  (stock / stock_info_v2 / stock_available).
- Produk tanpa model DAN tanpa item_sku → tetap 0 variant.
- Format normalized APISIX ("products" key) dengan & tanpa skus.
"""

from infrastructure.marketplace.apisix.parsers.shopee import ShopeeParser


def _raw_payload_with_models():
    """Item "Logo Mirorim" — has_model=true, 2 model (8F14-Pack-100, 8F14)."""
    return {
        "response": {
            "item": [
                {
                    "item_id": 802023254,
                    "item_status": "NORMAL",
                    "item_sku": "",
                    "_detail": {
                        "item_name": "Logo Mirorim",
                        "category_id": 300168,
                    },
                    "_model_raw": {
                        "model": [
                            {
                                "model_id": 11250494,
                                "model_sku": "8F14-Pack-100",
                                "model_name": "red",
                                "stock_info_v2": {
                                    "seller_stock": [
                                        {"location_id": "IDZ", "stock": 36}
                                    ],
                                    "summary_info": {
                                        "total_available_stock": 36,
                                        "total_reserved_stock": 0,
                                    },
                                },
                            },
                            {
                                "model_id": 11250495,
                                "model_sku": "8F14",
                                "model_name": "blue",
                                "stock_info_v2": {
                                    "seller_stock": [
                                        {"location_id": "IDZ", "stock": 4402}
                                    ],
                                    "summary_info": {
                                        "total_available_stock": 4402,
                                        "total_reserved_stock": 0,
                                    },
                                },
                            },
                        ],
                    },
                }
            ]
        }
    }


def _raw_payload_without_models():
    """Item "Diagram" — has_model=false, model=[], item_sku="22A4"."""
    return {
        "response": {
            "item": [
                {
                    "item_id": 802720223,
                    "item_status": "NORMAL",
                    "item_sku": "22A4",
                    "stock": 98,
                    "stock_available": 98,
                    "stock_reserved": 0,
                    "stock_info_v2": {
                        "seller_stock": [{"location_id": "IDZ", "stock": 98}],
                        "summary_info": {
                            "total_available_stock": 98,
                            "total_reserved_stock": 0,
                        },
                    },
                    "_detail": {
                        "item_name": "Diagram",
                        "category_id": 107022,
                        "has_model": False,
                    },
                    "_model_raw": {
                        "tier_variation": [],
                        "model": [],
                        "standardise_tier_variation": [],
                    },
                }
            ]
        }
    }


class TestRawFormatWithModels:
    def test_parses_models_as_variants(self):
        parser = ShopeeParser()
        products = parser.parse_products(_raw_payload_with_models())

        assert len(products) == 1
        product = products[0]
        assert product.external_product_id == "802023254"
        assert len(product.variants) == 2

        by_id = {v.external_variant_id: v for v in product.variants}
        assert set(by_id) == {"11250494", "11250495"}
        assert by_id["11250495"].seller_sku == "8F14"
        assert by_id["11250495"].available_stock == 4402
        assert by_id["11250495"].stock == 4402
        assert by_id["11250495"].location_id == "IDZ"
        assert by_id["11250494"].seller_sku == "8F14-Pack-100"

    def test_item_sku_empty_not_used_when_models_exist(self):
        """Item dgn model TIDAK boleh jatuh ke fallback item-as-variant."""
        parser = ShopeeParser()
        products = parser.parse_products(_raw_payload_with_models())
        variants = products[0].variants
        assert all(v.seller_sku in {"8F14", "8F14-Pack-100"} for v in variants)


class TestRawFormatWithoutModels:
    def test_falls_back_to_item_sku_single_variant(self):
        """
        Produk tanpa model (has_model=false) → 1 variant sintetis dengan
        seller_sku = item_sku ("22A4") dan stock level item (98).
        """
        parser = ShopeeParser()
        products = parser.parse_products(_raw_payload_without_models())

        assert len(products) == 1
        variants = products[0].variants
        assert len(variants) == 1

        variant = variants[0]
        assert variant.external_variant_id == "802720223"
        assert variant.seller_sku == "22A4"
        assert variant.stock == 98
        assert variant.available_stock == 98
        assert variant.reserved_stock == 0
        assert variant.location_id == "IDZ"

    def test_without_item_sku_keeps_zero_variants(self):
        """Tanpa item_sku tidak ada yang bisa dijadikan seller_sku → 0 variant."""
        payload = _raw_payload_without_models()
        payload["response"]["item"][0]["item_sku"] = ""
        payload["response"]["item"][0]["_detail"]["item_sku"] = ""

        parser = ShopeeParser()
        products = parser.parse_products(payload)

        assert len(products) == 1
        assert products[0].variants == []


class TestNormalizedFormat:
    def _normalized_with_skus(self):
        return {
            "products": [
                {
                    "external_product_id": "802023254",
                    "product_name": "Logo Mirorim",
                    "status": "NORMAL",
                    "skus": [
                        {
                            "external_variant_id": "11250495",
                            "seller_sku": "8F14",
                            "total_stock": 4402,
                            "total_reserved_stock": 0,
                            "total_available_stock": 4402,
                            "location_id": "IDZ",
                        }
                    ],
                }
            ]
        }

    def _normalized_without_skus(self):
        """
        Shape NYATA dari APISIX utk produk Shopee tanpa model:
        `skus` = {} (dict kosong), stock di level item via `inventory[]`,
        `total_available`, `total_reserved`, `location_id` — TANPA field
        `seller_sku`/`item_sku` di level item.
        """
        return {
            "products": [
                {
                    "inventory": [
                        {
                            "location_id": "IDZ",
                            "reserved_stock": 0,
                            "warehouse_id": "",
                            "available_stock": 98,
                        }
                    ],
                    "description": "...",
                    "product_name": "Diagram",
                    "location_id": "IDZ",
                    "external_product_id": "802720223",
                    "total_reserved": 0,
                    "category": "107022",
                    "total_available": 98,
                    "status": "ACTIVE",
                    "skus": {},
                }
            ]
        }

    def _normalized_with_skus_dict(self):
        """skus berbentuk dict BERISI field (bukan list) — beberapa normalizer."""
        return {
            "products": [
                {
                    "external_product_id": "802720223",
                    "product_name": "Diagram",
                    "status": "ACTIVE",
                    "skus": {
                        "seller_sku": "22A4",
                        "external_variant_id": "802720223",
                        "total_available_stock": 98,
                        "total_reserved_stock": 0,
                        "location_id": "IDZ",
                    },
                }
            ]
        }

    def _normalized_item_level_sku_with_stock(self):
        """
        skus = {} kosong + seller_sku di level item + stock di level item
        (inventory[] / total_available / total_reserved / location_id) —
        shape yang mengeksekusi fallback item-as-variant sepenuhnya.
        """
        return {
            "products": [
                {
                    "inventory": [
                        {
                            "location_id": "IDZ",
                            "reserved_stock": 0,
                            "warehouse_id": "",
                            "available_stock": 98,
                        }
                    ],
                    "description": "...",
                    "product_name": "Diagram",
                    "location_id": "IDZ",
                    "external_product_id": "802720223",
                    "total_reserved": 0,
                    "category": "107022",
                    "total_available": 98,
                    "status": "ACTIVE",
                    "seller_sku": "22A4",
                    "skus": {},
                }
            ]
        }

    def test_normalized_with_skus(self):
        parser = ShopeeParser()
        products = parser.parse_products(self._normalized_with_skus())
        assert len(products) == 1
        assert len(products[0].variants) == 1
        assert products[0].variants[0].seller_sku == "8F14"
        assert products[0].variants[0].external_variant_id == "11250495"

    def test_normalized_without_skus_keeps_zero_variants_no_sku(self):
        """
        Shape NYATA: skus={} & tanpa seller_sku/item_sku di level item →
        tidak ada yang bisa dijadikan seller_sku → 0 variant (defensif).
        """
        parser = ShopeeParser()
        products = parser.parse_products(self._normalized_without_skus())
        assert len(products) == 1
        assert products[0].variants == []

    def test_normalized_with_skus_dict_parsed_as_single_sku(self):
        """
        skus dict berisi field → diparse sebagai 1 sku (bukan fallback item).
        """
        parser = ShopeeParser()
        products = parser.parse_products(self._normalized_with_skus_dict())
        assert len(products) == 1
        variants = products[0].variants
        assert len(variants) == 1
        assert variants[0].seller_sku == "22A4"
        assert variants[0].external_variant_id == "802720223"
        assert variants[0].available_stock == 98
        assert variants[0].location_id == "IDZ"

    def test_normalized_item_level_sku_with_stock_falls_back(self):
        """
        skus={} + seller_sku/item_sku di level item + stock di level item
        (inventory[]/total_available/total_reserved/location_id) → fallback
        item-as-variant memakai stock level item.
        """
        parser = ShopeeParser()
        products = parser.parse_products(self._normalized_item_level_sku_with_stock())
        assert len(products) == 1
        variants = products[0].variants
        assert len(variants) == 1
        variant = variants[0]
        assert variant.seller_sku == "22A4"
        assert variant.external_variant_id == "802720223"
        assert variant.available_stock == 98
        assert variant.reserved_stock == 0
        assert variant.stock == 98
        assert variant.location_id == "IDZ"

    def test_normalized_skus_list_with_empty_seller_sku_falls_back(self):
        """
        skus list berisi entry dgn seller_sku kosong (mis. item tanpa model
        yang di-serialize APISIX sbg [{seller_sku: ""}]) → entry tsb dianggap
        tidak berguna; fallback item-as-variant dipakai bila ada item-level
        seller_sku/item_sku.
        """
        payload = self._normalized_with_skus_dict()
        # Ubah skus menjadi list berisi satu entry kosong (bukan dict berisi field)
        payload["products"][0]["skus"] = [
            {"external_variant_id": "0", "seller_sku": "", "status": "NORMAL"}
        ]
        # Item-level seller_sku tetap ada (shape APISIX saat item_sku di-set)
        payload["products"][0]["seller_sku"] = "22A4"
        parser = ShopeeParser()
        products = parser.parse_products(payload)
        assert len(products) == 1
        variants = products[0].variants
        assert len(variants) == 1
        assert variants[0].seller_sku == "22A4"
        assert variants[0].external_variant_id == "802720223"
