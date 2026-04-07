from __future__ import annotations

import math
from collections import defaultdict
from typing import Any


def calc_size_share(sales_of_size: float, sales_of_model: float) -> float:
    if sales_of_model <= 0:
        return 0.0
    return max(0.0, sales_of_size / sales_of_model)


def calc_buyout_pct(sold_units: float, ordered_units: float, min_buyout: float) -> float:
    if ordered_units <= 0:
        return min_buyout
    value = sold_units / ordered_units
    return min(1.0, max(0.0, value))


def calc_cover_days(total_cover_stock: float, avg_daily_orders: float, buyout_pct: float, min_buyout: float) -> float:
    effective = avg_daily_orders * max(buyout_pct, min_buyout)
    if effective <= 0:
        return 9999.0
    return total_cover_stock / effective


def calc_recommended_qty(target_cover_days: float, effective_daily_sales: float, total_cover_stock: float, round_to: int = 1) -> float:
    raw = max(target_cover_days * effective_daily_sales - total_cover_stock, 0)
    if round_to <= 1:
        return raw
    return math.ceil(raw / round_to) * round_to


def calc_priority(cover_days: float, critical: float, high: float, medium: float) -> int:
    if cover_days < critical:
        return 1
    if cover_days < high:
        return 2
    if cover_days < medium:
        return 3
    return 4


def build_priority_table(canonical: list[dict[str, Any]], rules: dict, warehouse_cfg: dict) -> list[dict[str, Any]]:
    if not canonical:
        return []

    cut_wh = set(warehouse_cfg["warehouse_categories"].get("cutting_room", []))
    ip_wh = set(warehouse_cfg["warehouse_categories"].get("finished_goods_ip", []))
    by_key: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in canonical:
        k = (row.get("article", ""), row.get("color", ""), row.get("size", ""), row.get("model_name", ""), row.get("product_name", ""))
        by_key[k].append(row)

    out_rows: list[dict[str, Any]] = []
    for (article, color, size, model_name, product_name), g in by_key.items():
        onec = [r for r in g if r.get("source_system") == "1C"]
        wb = [r for r in g if r.get("source_system") == "WB"]
        oz = [r for r in g if r.get("source_system") == "Ozon"]

        cut_qty = sum(r.get("qty", 0) for r in onec if r.get("warehouse_name") in cut_wh)
        ip_qty = sum(r.get("qty", 0) for r in onec if r.get("warehouse_name") in ip_wh)
        status_sum = lambda rows, st: sum(r.get("qty", 0) for r in rows if r.get("stock_status") == st)

        wb_sell, wb_to, wb_from, wb_transit = status_sum(wb, "sellable"), status_sum(wb, "to_client"), status_sum(wb, "from_client"), status_sum(wb, "in_transit_to_warehouse")
        oz_sell, oz_to, oz_from, oz_transit = status_sum(oz, "sellable"), status_sum(oz, "to_client"), status_sum(oz, "from_client"), status_sum(oz, "in_transit_to_warehouse")
        wb_total, oz_total = wb_sell + wb_to + wb_from + wb_transit, oz_sell + oz_to + oz_from + oz_transit

        orders = sum(r.get("orders", 0) for r in g)
        sales = sum(r.get("sales", 0) for r in g)
        model_rows = [r for r in canonical if r.get("article") == article]
        model_sales = sum(r.get("sales", 0) for r in model_rows)
        model_sizes = len({r.get("size", "") for r in model_rows if r.get("size")}) or 1

        size_share = calc_size_share(sales, model_sales) if model_sales > 0 else 1 / model_sizes
        if size_share == 0:
            size_share = 1 / model_sizes

        avg_daily_orders = orders / max(float(rules["report_days_window"]), 1.0)
        buyout = calc_buyout_pct(sales, orders, rules["min_buyout_pct"])
        total_stock = cut_qty + ip_qty + wb_total + oz_total
        cover = calc_cover_days(total_stock, avg_daily_orders, buyout, rules["min_buyout_pct"])
        eff = avg_daily_orders * max(buyout, rules["min_buyout_pct"])
        rec_model = calc_recommended_qty(rules["target_cover_days"], eff, total_stock, rules["production"]["round_to_pack"])
        rec_size = math.ceil(rec_model * size_share)
        priority = calc_priority(cover, rules["critical_cover_days"], rules["high_cover_days"], rules["medium_cover_days"])
        source_deficit = "Оба маркетплейса" if wb_total < 5 and oz_total < 5 else ("WB" if wb_total < oz_total else "Ozon")

        out_rows.append({
            "Номенклатура, Артикул": f"{product_name}, {article}",
            "Характеристика": size,
            "Остатки Цеховая кладовая": cut_qty,
            "СРОЧНО": rec_size,
            "ПРИОРИТЕТ": priority,
            "Остатки ИП": ip_qty,
            "Остатки на WB": wb_sell,
            "к клиенту": wb_to,
            "от клиента": wb_from,
            "в пути на склад": wb_transit,
            "Итого остатков на WB": wb_total,
            "Разбивка по размерам в % соотношении": round(size_share, 4),
            "Ср. скорость заказов в день": round(avg_daily_orders, 4),
            "Процент выкупа": round(buyout, 4),
            "На сколько дней хватает": round(cover, 2),
            "Остатки на Ozon": oz_sell,
            "к клиенту Ozon": oz_to,
            "от клиента Ozon": oz_from,
            "в пути на склад Ozon": oz_transit,
            "Итого остатков на Ozon": oz_total,
            "Итого по маркетплейсам": wb_total + oz_total,
            "Рекомендация к пошиву": rec_size,
            "Источник дефицита": source_deficit,
            "Комментарий проверки": "buyout fallback" if orders <= 0 else "",
        })

    return sorted(out_rows, key=lambda r: (r["ПРИОРИТЕТ"], -r["СРОЧНО"]))
