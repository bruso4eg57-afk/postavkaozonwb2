from __future__ import annotations

import hashlib
from typing import Any


class OzonClient:
    def __init__(self, client_id: str, api_key: str) -> None:
        self.client_id = client_id
        self.api_key = api_key

    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        data: list[dict[str, Any]] = [
            {"article": "ART001", "size": "M", "color": "Black", "barcode": "111", "sku": "oz-01", "status": "sellable", "qty": 12, "orders": 22, "sales": 16},
            {"article": "ART001", "size": "L", "color": "Black", "barcode": "112", "sku": "oz-02", "status": "from_client", "qty": 2, "orders": 14, "sales": 8},
            {"article": "ART003", "size": "XL", "color": "Blue", "barcode": "331", "sku": "oz-03", "status": "to_client", "qty": 6, "orders": 10, "sales": 4},
        ]
        return data, hashlib.md5(str(data).encode()).hexdigest()
