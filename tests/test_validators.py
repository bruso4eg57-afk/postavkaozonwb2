from src.services.validate import validate_canonical


def test_duplicate_and_negative_validation():
    rows = [
        {"source_system": "WB", "article": "A", "color": "B", "size": "M", "barcode": "1", "warehouse_name": "WB", "stock_status": "sellable", "qty": -1},
        {"source_system": "WB", "article": "A", "color": "B", "size": "M", "barcode": "1", "warehouse_name": "WB", "stock_status": "sellable", "qty": -1},
    ]
    issues = validate_canonical(rows)
    names = [i["check_name"] for i in issues]
    assert "duplicates" in names
    assert "negative_stock" in names
