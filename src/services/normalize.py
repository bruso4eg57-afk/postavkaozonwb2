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


def _canonicalize_key(key: str) -> str:
    return str(key).strip().lower().replace(" ", "").replace("_", "")


def _pick_field(row: dict[str, Any], candidates: list[str], default: Any = "") -> Any:
    if not candidates:
        return default
    cmap = {_canonicalize_key(k): v for k, v in row.items()}
    for c in candidates:
        ck = _canonicalize_key(c)
        if ck in cmap and cmap[ck] not in (None, ""):
            return cmap[ck]
    return default


def normalize_1c(records: list[dict[str, Any]], field_mapping: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fields_cfg = (field_mapping or {}).get("fields", {})

    def cands(name: str, defaults: list[str]) -> list[str]:
        value = fields_cfg.get(name, defaults)
        return value if isinstance(value, list) else defaults

    for r in records:
        product_name = str(_pick_field(r, cands("product_name", ["product_name", "Номенклатура"]), ""))
        article = str(_pick_field(r, cands("article", ["article", "Артикул", "sku"]), ""))
        characteristic = str(_pick_field(r, cands("characteristic", ["characteristic", "Характеристика"]), ""))
        size = str(_pick_field(r, cands("size", ["size", "Размер"]), ""))
        barcode = str(_pick_field(r, cands("barcode", ["barcode", "Штрихкод"]), ""))

        if not article and product_name:
            article = product_name[:64]
        if not size and characteristic:
            size = characteristic

        rows.append(
            {
                "source_system": "1C",
                "organization": _pick_field(r, cands("organization", ["organization"]), "Unknown"),
                "marketplace": "1C",
                "article": article,
                "model_name": str(_pick_field(r, cands("model_name", ["model_name", "Модель"]), "")) or product_name,
                "product_name": product_name,
                "color": str(_pick_field(r, cands("color", ["color", "Цвет"]), "")),
                "size": size,
                "characteristic": characteristic,
                "barcode": barcode,
                "warehouse_name": str(_pick_field(r, cands("warehouse_name", ["warehouse_name", "Склад"]), "")),
                "stock_status": "sellable",
                "qty": float(_pick_field(r, cands("qty", ["qty", "quantity", "Остаток"]), 0) or 0),
                "onec_nomenclature": product_name,
                "orders": float(_pick_field(r, ["orders"], 0) or 0),
                "sales": float(_pick_field(r, ["sales"], 0) or 0),
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

    unresolved = [r for r in deduped if (not r.get("article") and not r.get("barcode")) or (not r.get("size") and not r.get("characteristic"))]
    return NormalizeResult(canonical=deduped, unresolved=unresolved, removed_duplicates=removed)
