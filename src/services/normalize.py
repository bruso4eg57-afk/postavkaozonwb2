from __future__ import annotations

from dataclasses import dataclass
from typing import Any


STATUS_MAP = {
    "available": "sellable",
    "sellable": "sellable",
    "to_client": "to_client",
    "from_client": "from_client",
    "in_transit_to_warehouse": "in_transit_to_warehouse",
    "in_transit": "in_transit_to_warehouse",
}


@dataclass
class NormalizeResult:
    canonical: list[dict[str, Any]]
    unresolved: list[dict[str, Any]]
    removed_duplicates: int = 0


def _norm_status(value: str) -> str:
    return STATUS_MAP.get(str(value).strip().lower(), "sellable")


def _row_signature(row: dict[str, Any]) -> tuple:
    return tuple(sorted((str(k), str(v)) for k, v in row.items()))


def normalize_1c(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for r in records:
        rows.append(
            {
                "source_system": "1C",
                "organization": r.get("organization", "Unknown"),
                "marketplace": "1C",
                "article": r.get("article", ""),
                "model_name": r.get("model_name", ""),
                "product_name": r.get("product_name", ""),
                "color": r.get("color", ""),
                "size": r.get("size", ""),
                "characteristic": r.get("characteristic", ""),
                "barcode": str(r.get("barcode", "")),
                "warehouse_name": r.get("warehouse_name", ""),
                "stock_status": "sellable",
                "qty": float(r.get("qty", 0) or 0),
                "onec_nomenclature": r.get("product_name", ""),
                "orders": float(r.get("orders", 0) or 0),
                "sales": float(r.get("sales", 0) or 0),
            }
        )
    return rows


def normalize_mp(records: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
    rows = []
    marketplace = "WB" if source == "WB" else "Ozon"
    for r in records:
        rows.append(
            {
                "source_system": source,
                "organization": "ИП",
                "marketplace": marketplace,
                "article": r.get("article", ""),
                "model_name": r.get("model_name", r.get("article", "")),
                "product_name": r.get("product_name", r.get("article", "")),
                "color": r.get("color", ""),
                "size": r.get("size", ""),
                "characteristic": r.get("characteristic", ""),
                "barcode": str(r.get("barcode", "")),
                "warehouse_name": marketplace,
                "stock_status": _norm_status(r.get("status", "sellable")),
                "qty": float(r.get("qty", 0) or 0),
                "ozon_sku": r.get("sku", "") if source == "Ozon" else "",
                "wb_nm_id": str(r.get("nm_id", "")) if source == "WB" else "",
                "orders": float(r.get("orders", 0) or 0),
                "sales": float(r.get("sales", 0) or 0),
            }
        )
    return rows


def unify_sku(onec: list[dict[str, Any]], wb: list[dict[str, Any]], ozon: list[dict[str, Any]], aliases: dict[str, str]) -> NormalizeResult:
    raw_rows = [*onec, *wb, *ozon]
    for row in raw_rows:
        row["sku_key"] = str(row.get("barcode") or "") or f"{row.get('article', '')}|{row.get('size', '')}|{row.get('color', '')}" or aliases.get(row.get("article", ""), "")

    deduped: list[dict[str, Any]] = []
    seen = set()
    removed = 0
    for row in raw_rows:
        sig = _row_signature(row)
        if sig in seen:
            removed += 1
            continue
        seen.add(sig)
        deduped.append(row)

    unresolved = [r for r in deduped if not r.get("article") or not r.get("size")]
    return NormalizeResult(canonical=deduped, unresolved=unresolved, removed_duplicates=removed)
