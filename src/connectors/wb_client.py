from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.request import Request, urlopen


class WbClient:
    def __init__(self, token: str, report_days_window: int = 30) -> None:
        self.token = token
        self.report_days_window = report_days_window

    def _get_json(self, url: str) -> list[dict[str, Any]]:
        auth_variants = [self.token, f"Bearer {self.token}"]
        last_exc = None
        for auth in auth_variants:
            try:
                req = Request(url)
                req.add_header("Authorization", auth)
                with urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode("utf-8")
                data = json.loads(raw)
                return data if isinstance(data, list) else []
            except Exception as exc:
                last_exc = exc
                continue
        raise RuntimeError(str(last_exc))


    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        # Demo mode fallback
        if not self.token:
            data: list[dict[str, Any]] = [
                {"article": "ART001", "size": "M", "color": "Black", "barcode": "111", "nm_id": "9001", "status": "available", "qty": 25, "orders": 40, "sales": 26},
                {"article": "ART001", "size": "L", "color": "Black", "barcode": "112", "nm_id": "9001", "status": "to_client", "qty": 5, "orders": 20, "sales": 10},
                {"article": "ART002", "size": "S", "color": "White", "barcode": "221", "nm_id": "9002", "status": "in_transit_to_warehouse", "qty": 8, "orders": 15, "sales": 8},
            ]
            return data, hashlib.md5(str(data).encode()).hexdigest()

        date_from = (datetime.now(timezone.utc) - timedelta(days=self.report_days_window)).strftime("%Y-%m-%dT00:00:00")
        stocks = self._get_json(f"https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom={date_from}")
        orders = self._get_json(f"https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom={date_from}")
        sales = self._get_json(f"https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom={date_from}")

        agg: dict[tuple[str, str, str], dict[str, Any]] = {}

        def key_of(r: dict[str, Any]) -> tuple[str, str, str]:
            return (str(r.get("supplierArticle", "")), str(r.get("techSize", "")), str(r.get("barcode", "")))

        for r in stocks:
            k = key_of(r)
            rec = agg.setdefault(k, {"article": k[0], "size": k[1], "color": str(r.get("subject", "")), "barcode": k[2], "nm_id": str(r.get("nmId", "")), "status": "available", "qty": 0.0, "orders": 0.0, "sales": 0.0})
            rec["qty"] += float(r.get("quantity", 0) or 0)

        for r in orders:
            k = key_of(r)
            rec = agg.setdefault(k, {"article": k[0], "size": k[1], "color": str(r.get("subject", "")), "barcode": k[2], "nm_id": str(r.get("nmId", "")), "status": "to_client", "qty": 0.0, "orders": 0.0, "sales": 0.0})
            rec["orders"] += 1

        for r in sales:
            k = key_of(r)
            rec = agg.setdefault(k, {"article": k[0], "size": k[1], "color": str(r.get("subject", "")), "barcode": k[2], "nm_id": str(r.get("nmId", "")), "status": "sellable", "qty": 0.0, "orders": 0.0, "sales": 0.0})
            rec["sales"] += 1

        result = list(agg.values())
        return result, hashlib.md5(json.dumps(result, ensure_ascii=False).encode()).hexdigest()
