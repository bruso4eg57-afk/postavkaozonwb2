from src.services.calculate import (
    calc_buyout_pct,
    calc_cover_days,
    calc_priority,
    calc_recommended_qty,
    calc_size_share,
)


def test_size_share():
    assert calc_size_share(10, 40) == 0.25


def test_buyout_pct():
    assert calc_buyout_pct(5, 10, 0.45) == 0.5
    assert calc_buyout_pct(0, 0, 0.45) == 0.45


def test_cover_days():
    assert round(calc_cover_days(100, 5, 0.5, 0.45), 2) == 40.0


def test_recommended_qty():
    assert calc_recommended_qty(21, 3, 20, 5) == 45


def test_priority():
    assert calc_priority(6, 7, 14, 21) == 1
    assert calc_priority(15, 7, 14, 21) == 3
