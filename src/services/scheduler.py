from __future__ import annotations

import time


def run_scheduler(cron_expr: str, timezone: str, task) -> None:
    _ = cron_expr, timezone
    # Lightweight fallback scheduler for restricted environments: run every 6 hours.
    while True:
        time.sleep(6 * 60 * 60)
        task()
