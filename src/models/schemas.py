from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


class CanonicalSkuRecord(BaseModel):
    source_system: str
    organization: str = "Unknown"
    marketplace: str = "Unknown"
    article: str = ""
    model_name: str = ""
    product_name: str = ""
    color: str = ""
    size: str = ""
    characteristic: str = ""
    barcode: str = ""
    ozon_sku: str = ""
    wb_nm_id: str = ""
    onec_nomenclature: str = ""
    warehouse_name: str = ""
    stock_status: str = "sellable"
    qty: float = 0.0
    order_date: datetime | None = None
    sale_date: datetime | None = None
    return_date: datetime | None = None
    source_payload_hash: str = ""
    refreshed_at: datetime = Field(default_factory=datetime.utcnow)
