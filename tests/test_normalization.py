from src.services.normalize import normalize_1c, normalize_mp, unify_sku


def test_normalization_and_unknown_fields():
    onec = normalize_1c([{"article": "A", "size": "M", "qty": 1, "warehouse_name": "Цеховая кладовая", "unknown": "x"}])
    wb = normalize_mp([{"article": "A", "size": "M", "status": "available", "qty": 2}], "WB")
    oz = normalize_mp([{"article": "A", "size": "M", "status": "sellable", "qty": 3}], "Ozon")
    res = unify_sku(onec, wb, oz, {})
    assert len(res.canonical) == 3
    assert "sku_key" in res.canonical[0]


def test_unify_deduplicates_exact_rows():
    onec = normalize_1c([{"article": "A", "size": "M", "qty": 1, "warehouse_name": "Цеховая кладовая"}])
    res = unify_sku(onec, onec.copy(), [], {})
    assert len(res.canonical) == 1
    assert res.removed_duplicates >= 1
