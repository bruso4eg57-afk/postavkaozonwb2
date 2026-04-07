from __future__ import annotations

from datetime import datetime
from typing import Any

REQUIRED_COLUMNS = ["article", "size", "qty", "source_system", "stock_status"]


def validate_canonical(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    now = datetime.utcnow().isoformat()

    present = set()
    for row in rows:
        present.update(row.keys())
    for col in REQUIRED_COLUMNS:
        if col not in present:
            issues.append({"ts": now, "level": "error", "check_name": "required_columns", "details": f"missing: {col}"})

    seen = set()
    dup_count = 0
    neg_count = 0
    for row in rows:
        key = tuple(row.get(k, "") for k in ["source_system", "article", "color", "size", "barcode", "warehouse_name", "stock_status"])
        if key in seen:
            dup_count += 1
        else:
            seen.add(key)
        if float(row.get("qty", 0) or 0) < 0:
            neg_count += 1

    if dup_count > 0:
        issues.append({"ts": now, "level": "warning", "check_name": "duplicates", "details": f"duplicates={dup_count}"})
    if neg_count > 0:
        issues.append({"ts": now, "level": "warning", "check_name": "negative_stock", "details": f"negative_rows={neg_count}"})
    if not issues:
        issues.append({"ts": now, "level": "info", "check_name": "validation", "details": "OK"})
    return issues
