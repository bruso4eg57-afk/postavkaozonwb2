from __future__ import annotations

import hashlib
from typing import Any


class WbClient:
    def __init__(self, token: str) -> None:
        self.token = token

    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        # Demo mode if no token. Real API integration can be added without changing normalization layer.
        data: list[dict[str, Any]] = [
            {"article": "ART001", "size": "M", "color": "Black", "barcode": "111", "nm_id": "9001", "status": "available", "qty": 25, "orders": 40, "sales": 26},
            {"article": "ART001", "size": "L", "color": "Black", "barcode": "112", "nm_id": "9001", "status": "to_client", "qty": 5, "orders": 20, "sales": 10},
            {"article": "ART002", "size": "S", "color": "White", "barcode": "221", "nm_id": "9002", "status": "in_transit_to_warehouse", "qty": 8, "orders": 15, "sales": 8},
        ]
        return data, hashlib.md5(str(data).encode()).hexdigest()
