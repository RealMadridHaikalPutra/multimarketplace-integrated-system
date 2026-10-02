"""Dummy APISIX gateway — meniru endpoint yang dipakai alur webhook Camunda.

Dipakai saat APISIX ASLI mati: verifier menunjuk ``settings.apisix_base_url``
ke server ini, sehingga kode produksi (ApisixClient) bisa berjalan tanpa
gateway sungguhan.

Endpoint yang dilayani (semua GET kecuali ditulis lain):

    GET  /order?marketplace=..&shop_uuid=..&ids=<order_id>&translate=false
         → DETAIL ORDER mentah (TikTok: data.orders[].line_items[])

    GET  /order?marketplace=..&shop_uuid=..&product_id=<pid>
         → reserved stock per produk (filtered_by="product_id")

    GET  /products?...                 → [] (tidak dipakai alur webhook)
    POST /update-stock /update-status  → {"ok": true}
    GET  /health, GET /__state         → status & isi internal

Jalankan mandiri:
    python -m dummy.dummy_apisix --port 9080
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse


class DummyApisix:
    """Server APISIX palsu (stdlib) yang bisa dijalankan dalam thread."""

    def __init__(
        self,
        *,
        marketplace: str = "tiktok",
        order_id: str = "ORDER-DUMMY-1",
        order_status: str = "UNPAID",
        product_id: str = "PROD-DUMMY-1",
        variant_id: str = "VAR-DUMMY-1",
        seller_sku: str = "1A1",
        product_name: str = "Kaos Polos Dummy",
        variant_name: str = "Single Pack",
        quantity: int = 2,
        reserved: int = 3,
        port: int = 0,
    ) -> None:
        self.marketplace = marketplace
        self.order_id = order_id
        self.order_status = order_status
        self.product_id = product_id
        self.variant_id = variant_id
        self.seller_sku = seller_sku
        self.product_name = product_name
        self.variant_name = variant_name
        self.quantity = quantity
        self.reserved = reserved
        self.requests: List[Dict[str, Any]] = []

        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._requested_port = port

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> str:
        """Jalankan server di thread. @return base URL, mis. http://127.0.0.1:1234"""
        handler = self._build_handler()
        self._server = ThreadingHTTPServer(("127.0.0.1", self._requested_port), handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        host, port = self._server.server_address[:2]
        self.base_url = f"http://{host}:{port}"
        return self.base_url

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=3)
            self._thread = None

    # ------------------------------------------------------------------
    # Payload helpers
    # ------------------------------------------------------------------

    def order_detail_payload(self) -> Dict[str, Any]:
        """DETAIL ORDER mentah (TikTok: data.orders[].line_items[])."""
        return {
            "data": {
                "orders": [
                    {
                        "id": self.order_id,
                        "order_status": self.order_status,
                        "status": self.order_status,
                        "line_items": [
                            {
                                "product_id": self.product_id,
                                "product_name": self.product_name,
                                "sku_name": self.variant_name,
                                "seller_sku": self.seller_sku,
                                "quantity": self.quantity,
                                "sale_price": "29900",
                            }
                        ],
                    }
                ]
            }
        }

    def reserved_payload(self, product_id: str) -> Dict[str, Any]:
        """Reserved stock satu produk (bentuk terstandardisasi APISIX)."""
        return {
            "reserved_stock": [
                {
                    "product_id": str(product_id),
                    "variants": [
                        {
                            "variant_id": self.variant_id,
                            "seller_sku": self.seller_sku,
                            "total_reserved_stock": self.reserved,
                        }
                    ],
                }
            ],
            "filtered_by": "product_id",
        }

    # ------------------------------------------------------------------
    # HTTP handler
    # ------------------------------------------------------------------

    def _build_handler(self):
        outer = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # noqa: D401
                return  # senyap

            def _send_json(self, payload: Any, status: int = 200) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                params = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
                outer.requests.append({"method": "GET", "path": parsed.path, "params": params})

                if parsed.path == "/__state":
                    self._send_json({"requests": outer.requests})
                    return
                if parsed.path == "/health":
                    self._send_json({"status": "ok"})
                    return
                if parsed.path.endswith("/products"):
                    self._send_json({"products": [], "pagination": {"has_next": False}})
                    return
                if parsed.path.endswith("/order"):
                    if params.get("ids"):
                        self._send_json(outer.order_detail_payload())
                    elif params.get("product_id"):
                        self._send_json(outer.reserved_payload(params["product_id"]))
                    else:
                        self._send_json(outer.reserved_payload(outer.product_id))
                    return
                self._send_json({"error": "not found", "path": parsed.path}, status=404)

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                outer.requests.append({"method": "POST", "path": parsed.path, "raw": raw.decode("utf-8", "replace")})
                self._send_json({"ok": True, "marketplace": outer.marketplace})

        return _Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy APISIX gateway")
    parser.add_argument("--port", type=int, default=9080)
    parser.add_argument("--marketplace", default="tiktok")
    parser.add_argument("--order-id", default="ORDER-DUMMY-1")
    parser.add_argument("--product-id", default="PROD-DUMMY-1")
    parser.add_argument("--seller-sku", default="1A1")
    parser.add_argument("--reserved", type=int, default=3)
    args = parser.parse_args()

    server = DummyApisix(
        marketplace=args.marketplace,
        order_id=args.order_id,
        product_id=args.product_id,
        seller_sku=args.seller_sku,
        reserved=args.reserved,
        port=args.port,
    )
    base = server.start()
    print(f"Dummy APISIX berjalan di {base} (Ctrl+C untuk stop)")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
