from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError
import base64
import time


class OneCClient:
    def __init__(self, url: str, login: str, password: str) -> None:
        self.url = url
        self.login = login
        self.password = password

    def fetch(self) -> tuple[list[dict[str, Any]], str]:
        if not self.url:
            data = [
                {"article": "ART001", "model_name": "Базовая футболка", "product_name": "Футболка", "color": "Black", "size": "M", "barcode": "111", "warehouse_name": "Цеховая кладовая", "qty": 20, "organization": "ИП"},
                {"article": "ART001", "model_name": "Базовая футболка", "product_name": "Футболка", "color": "Black", "size": "L", "barcode": "112", "warehouse_name": "Склад ИП", "qty": 10, "organization": "ИП"},
            ]
            return data, hashlib.md5(str(data).encode()).hexdigest()

        last_exc = None
        for i in range(3):
            try:
                req = Request(self.url)
                if self.login:
                    token = base64.b64encode(f"{self.login}:{self.password}".encode()).decode()
                    req.add_header("Authorization", f"Basic {token}")
                with urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                records = payload if isinstance(payload, list) else payload.get("value") or payload.get("items") or payload.get("data") or []
                return records, hashlib.md5(json.dumps(payload, ensure_ascii=False).encode()).hexdigest()
            except URLError as exc:
                last_exc = exc
                time.sleep(2 ** i)
        raise RuntimeError(f"1C fetch failed: {last_exc}")
