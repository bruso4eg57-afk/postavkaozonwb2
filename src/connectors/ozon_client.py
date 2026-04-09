from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.request import Request, urlopen


class OzonClient:
    def __init__(self, client_id: str, api_key: str, report_days_window: int = 30) -> None:
        self.client_id = client_id
        self.api_key = api_key
        self.report_days_window = report_days_window

    def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        urls = [url]
        if '/v3/' in url:
            urls.append(url.replace('/v3/', '/v2/'))
        if '/v2/' in url:
            urls.append(url.replace('/v2/', '/v3/'))

        last_exc = None
        for u in urls:
            try:
                req = Request(u, data=json.dumps(payload).encode("utf-8"), method="POST")
                req.add_header("Client-Id", self.client_id)
                req.add_header("Api-Key", self.api_key)
                req.add_header("Content-Type", "application/json")
                with urlopen(req, timeout=40) as resp:
                    raw = resp.read().decode("utf-8")
                obj = json.loads(raw)
                return obj if isinstance(obj, dict) else {}
            except Exception as exc:
                last_exc = exc
                continue
        raise RuntimeError(str(last_exc))


    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        if not self.client_id or not self.api_key:
            data: list[dict[str, Any]] = [
                {"article": "ART001", "size": "M", "color": "Black", "barcode": "111", "sku": "oz-01", "status": "sellable", "qty": 12, "orders": 22, "sales": 16},
                {"article": "ART001", "size": "L", "color": "Black", "barcode": "112", "sku": "oz-02", "status": "from_client", "qty": 2, "orders": 14, "sales": 8},
                {"article": "ART003", "size": "XL", "color": "Blue", "barcode": "331", "sku": "oz-03", "status": "to_client", "qty": 6, "orders": 10, "sales": 4},
            ]
            return data, hashlib.md5(str(data).encode()).hexdigest()

        stocks_payload = {"filter": {"visibility": "ALL"}, "limit": 1000}
        stock_urls = [
            "https://api-seller.ozon.ru/v2/product/info/stocks",
            "https://api-seller.ozon.ru/v3/product/info/stocks",
            "https://api-seller.ozon.ru/v4/product/info/stocks",
            "https://api-seller.ozon.ru/v1/analytics/stock_on_warehouses",
            "https://api-seller.ozon.ru/v1/product/list",
        ]
        stocks_resp = {}
        for u in stock_urls:
            try:
                payload = stocks_payload
                if "stock_on_warehouses" in u:
                    payload = {"limit": 1000, "offset": 0, "warehouse_type": "ALL"}
                if u.endswith('/v1/product/list'):
                    payload = {"filter": {"visibility": "ALL"}, "last_id": "", "limit": 1000}
                stocks_resp = self._post_json(u, payload)
                if stocks_resp:
                    break
            except Exception:
                continue

        result_obj = stocks_resp.get("result")
        if isinstance(result_obj, dict):
            items = result_obj.get("items") or result_obj.get("rows") or []
        elif isinstance(result_obj, list):
            items = result_obj
        else:
            items = []

        # Optional postings to estimate orders/sales
        date_from = (datetime.now(timezone.utc) - timedelta(days=self.report_days_window)).strftime("%Y-%m-%dT00:00:00Z")
        postings_payload = {"dir": "ASC", "filter": {"since": date_from, "to": datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59Z")}, "limit": 1000, "offset": 0}
        postings = []
        posting_urls = [
            "https://api-seller.ozon.ru/v2/posting/fbo/list",
            "https://api-seller.ozon.ru/v3/posting/fbo/list",
            "https://api-seller.ozon.ru/v2/posting/fbs/list",
        ]
        for u in posting_urls:
            try:
                postings_resp = self._post_json(u, postings_payload)
                postings = postings_resp.get("result", []) if isinstance(postings_resp.get("result"), list) else []
                if postings:
                    break
            except Exception:
                continue

        by_offer: dict[str, dict[str, float]] = {}
        for p in postings:
            for pr in p.get("products", []) or []:
                offer = str(pr.get("offer_id", ""))
                s = by_offer.setdefault(offer, {"orders": 0.0, "sales": 0.0})
                s["orders"] += float(pr.get("quantity", 1) or 1)
                if p.get("status") in {"delivered", "awaiting_deliver", "delivering"}:
                    s["sales"] += float(pr.get("quantity", 1) or 1)

        data: list[dict[str, Any]] = []
        for it in items:
            offer = str(it.get("offer_id", ""))
            sku = str(it.get("product_id", ""))
            present = 0.0
            for st in it.get("stocks", []) or []:
                present += float(st.get("present", st.get("free_to_sell_amount", 0)) or 0)
            if present == 0:
                present = float(it.get("present", it.get("free_to_sell_amount", it.get("stock", 0))) or 0)
            extra = by_offer.get(offer, {"orders": 0.0, "sales": 0.0})
            data.append({
                "article": offer,
                "size": "",
                "color": "",
                "barcode": "",
                "sku": sku,
                "status": "sellable",
                "qty": present,
                "orders": extra["orders"],
                "sales": extra["sales"],
            })

        return data, hashlib.md5(json.dumps(data, ensure_ascii=False).encode()).hexdigest()
