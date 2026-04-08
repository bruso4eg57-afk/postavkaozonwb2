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


def test_normalize_1c_with_russian_field_names():
    mapping = {"fields": {"article": ["Артикул"], "size": ["Размер"], "qty": ["Остаток"], "warehouse_name": ["Склад"]}}
    rows = normalize_1c([{"Артикул": "RU-1", "Размер": "42", "Остаток": 5, "Склад": "Цех"}], mapping)
    assert rows[0]["article"] == "RU-1"
    assert rows[0]["size"] == "42"
    assert rows[0]["qty"] == 5.0


def test_normalize_1c_fallback_article_and_size():
    rows = normalize_1c([{"Номенклатура": "Платье X", "Характеристика": "44"}], {"fields": {"product_name": ["Номенклатура"], "characteristic": ["Характеристика"]}})
    assert rows[0]["article"] == "Платье X"
    assert rows[0]["size"] == "44"


def test_normalize_1c_name_code_defaults():
    rows = normalize_1c([{"name": "Юбка", "code": "ART-CODE", "characteristic": "46", "amount": 3, "warehouse": "Склад 1"}])
    assert rows[0]["product_name"] == "Юбка"
    assert rows[0]["article"] == "ART-CODE"
    assert rows[0]["size"] == "46"
    assert rows[0]["qty"] == 3.0
