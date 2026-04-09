from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

REQUIRED_COLUMNS = ["article", "size", "qty", "source_system", "stock_status"]


def _stable_row_signature(row: dict[str, Any]) -> tuple:
    return tuple(sorted((str(k), str(v)) for k, v in row.items()))


def validate_canonical(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    now = datetime.now(timezone.utc).isoformat()

    present = set()
    for row in rows:
        present.update(row.keys())
    for col in REQUIRED_COLUMNS:
        if col not in present:
            issues.append({"ts": now, "level": "error", "check_name": "required_columns", "details": f"missing: {col}"})

    exact_seen = set()
    exact_dup_count = 0
    neg_count = 0
    for row in rows:
        sig = _stable_row_signature(row)
        if sig in exact_seen:
            exact_dup_count += 1
        else:
            exact_seen.add(sig)

        try:
            qty = float(row.get("qty", 0) or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty < 0:
            neg_count += 1

    if exact_dup_count > 0:
        issues.append({"ts": now, "level": "warning", "check_name": "duplicates", "details": f"exact_duplicates={exact_dup_count}"})
    if neg_count > 0:
        issues.append({"ts": now, "level": "warning", "check_name": "negative_stock", "details": f"negative_rows={neg_count}"})
    if not issues:
        issues.append({"ts": now, "level": "info", "check_name": "validation", "details": "OK"})
    return issues
