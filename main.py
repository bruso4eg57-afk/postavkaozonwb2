from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from src.connectors.onec_client import OneCClient
from src.connectors.ozon_client import OzonClient
from src.connectors.wb_client import WbClient
from src.services.cache import CacheRepository
from src.services.calculate import build_priority_table
from src.services.export_excel import export_report
from src.services.normalize import normalize_1c, normalize_mp, unify_sku
from src.services.scheduler import run_scheduler
from src.services.validate import validate_canonical
from src.utils.logging_utils import setup_logging


def _load_dotenv() -> None:
    env_path = ".env"
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def _yaml_scalar(value: str):
    value = value.strip()
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass
    if value.startswith("[") and value.endswith("]"):
        return [x.strip().strip("'\"") for x in value[1:-1].split(",") if x.strip()]
    return value.strip("'\"")


def load_yaml(path: str) -> dict:
    # lightweight yaml reader for current config files
    result: dict = {}
    stack: list[tuple[int, dict]] = [(-1, result)]
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            if not raw.strip() or raw.lstrip().startswith("#"):
                continue
            indent = len(raw) - len(raw.lstrip(" "))
            line = raw.strip()
            while stack and indent <= stack[-1][0]:
                stack.pop()
            parent = stack[-1][1]
            if line.startswith("- "):
                continue
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            key = k.strip()
            val = v.strip()
            if not val:
                parent[key] = {}
                stack.append((indent, parent[key]))
            else:
                parent[key] = _yaml_scalar(val)
    # handle simple lists under known keys in warehouses.yaml
    if "warehouse_categories" in result:
        wc = result["warehouse_categories"]
        for name in list(wc.keys()):
            if isinstance(wc[name], dict):
                wc[name] = []
        current = None
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if line.endswith(":") and line[:-1] in wc:
                    current = line[:-1]
                elif line.startswith("-") and current:
                    wc[current].append(line[1:].strip())
    return result


def read_settings() -> dict:
    _load_dotenv()
    return {
        "ozon_client_id": os.getenv("OZON_CLIENT_ID", ""),
        "ozon_api_key": os.getenv("OZON_API_KEY", ""),
        "wb_token": os.getenv("WB_TOKEN", ""),
        "onec_url": os.getenv("ONEC_JSON_URL", ""),
        "onec_login": os.getenv("ONEC_AUTH_LOGIN", ""),
        "onec_password": os.getenv("ONEC_AUTH_PASSWORD", ""),
        "refresh_cron": os.getenv("REFRESH_CRON", "0 */6 * * *"),
        "timezone": os.getenv("TIMEZONE", "Europe/Moscow"),
        "cache_db_path": os.getenv("CACHE_DB_PATH", "data/cache.db"),
        "output_path": os.getenv("OUTPUT_PATH", "output/production_plan.xlsx"),
    }


def sync_data(settings: dict, cache: CacheRepository, logger):
    ts = datetime.now(timezone.utc).isoformat()
    onec_client = OneCClient(settings["onec_url"], settings["onec_login"], settings["onec_password"])
    wb_client = WbClient(settings["wb_token"])
    oz_client = OzonClient(settings["ozon_client_id"], settings["ozon_api_key"])

    def fetch_with_cache(source: str, fn):
        try:
            data, h = fn()
            cache.save_snapshot(source, ts, data, True, h)
            return data, False, ts
        except Exception as exc:
            logger.warning("%s unavailable, using cache: %s", source, exc)
            snap = cache.latest_success(source)
            if snap:
                return snap["payload"], True, snap["ts"]
            return [], True, ""

    onec_raw, onec_stale, onec_ts = fetch_with_cache("1C", onec_client.fetch)
    wb_raw, wb_stale, wb_ts = fetch_with_cache("WB", wb_client.fetch)
    oz_raw, oz_stale, oz_ts = fetch_with_cache("Ozon", oz_client.fetch)

    statuses = {
        "1C": {"stale": onec_stale, "ts": onec_ts},
        "WB": {"stale": wb_stale, "ts": wb_ts},
        "Ozon": {"stale": oz_stale, "ts": oz_ts},
    }
    return onec_raw, wb_raw, oz_raw, statuses


def build_pipeline(settings: dict):
    logger = setup_logging()
    cache = CacheRepository(settings["cache_db_path"])
    business = load_yaml("config/business_rules.yaml")
    warehouses = load_yaml("config/warehouses.yaml")
    aliases = load_yaml("config/sku_aliases.yaml").get("aliases", {})

    onec_raw, wb_raw, oz_raw, statuses = sync_data(settings, cache, logger)
    df_1c = normalize_1c(onec_raw)
    df_wb = normalize_mp(wb_raw, "WB")
    df_oz = normalize_mp(oz_raw, "Ozon")
    norm = unify_sku(df_1c, df_wb, df_oz, aliases)

    checks = validate_canonical(norm.canonical)
    for row in checks:
        cache.save_validation(row["ts"], row["level"], row["check_name"], row["details"])

    priority = build_priority_table(norm.canonical, business, warehouses)

    settings_rows = [
        {"key": "target_cover_days", "value": business.get("target_cover_days")},
        {"key": "critical_cover_days", "value": business.get("critical_cover_days")},
        {"key": "high_cover_days", "value": business.get("high_cover_days")},
        {"key": "medium_cover_days", "value": business.get("medium_cover_days")},
        {"key": "min_buyout_pct", "value": business.get("min_buyout_pct")},
    ]
    for src, st in statuses.items():
        settings_rows.append({"key": f"{src}_stale", "value": st["stale"]})
        settings_rows.append({"key": f"{src}_last_success", "value": st["ts"]})

    mapping_pct = 0 if not norm.canonical else round((1 - len(norm.unresolved) / len(norm.canonical)) * 100, 2)
    settings_rows.append({"key": "sku_mapping_pct", "value": mapping_pct})

    logs = [{"ts": datetime.now(timezone.utc).isoformat(), "level": "info", "message": "Pipeline completed"}]

    return {
        "priority": priority,
        "checks": checks,
        "unresolved": norm.unresolved,
        "raw_1c": onec_raw,
        "raw_wb": wb_raw,
        "raw_oz": oz_raw,
        "settings": settings_rows,
        "log": logs,
    }


def command_sync(_args):
    settings = read_settings()
    build_pipeline(settings)


def command_validate(_args):
    settings = read_settings()
    res = build_pipeline(settings)
    for row in res["checks"]:
        print(row)


def command_export(_args):
    settings = read_settings()
    res = build_pipeline(settings)
    export_report(
        settings["output_path"],
        res["priority"],
        res["checks"],
        res["unresolved"],
        res["raw_1c"],
        res["raw_wb"],
        res["raw_oz"],
        res["settings"],
        res["log"],
        template_path="остатки+ИП(1)_(1).xlsx",
    )
    print(f"Saved: {settings['output_path']}")


def command_run(_args):
    settings = read_settings()

    def task():
        res = build_pipeline(settings)
        export_report(
            settings["output_path"],
            res["priority"],
            res["checks"],
            res["unresolved"],
            res["raw_1c"],
            res["raw_wb"],
            res["raw_oz"],
            res["settings"],
            res["log"],
            template_path="остатки+ИП(1)_(1).xlsx",
        )

    task()
    run_scheduler(settings["refresh_cron"], settings["timezone"], task)


def command_backfill(args):
    settings = read_settings()
    cache = CacheRepository(settings["cache_db_path"])
    now = datetime.now(timezone.utc)
    for d in range(args.days):
        ts = (now - timedelta(days=d)).isoformat()
        cache.save_snapshot("backfill", ts, {"day": d}, True, str(d))
    print(f"Backfill inserted: {args.days}")


def build_parser():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync").set_defaults(func=command_sync)
    sub.add_parser("validate").set_defaults(func=command_validate)
    sub.add_parser("export").set_defaults(func=command_export)
    sub.add_parser("run").set_defaults(func=command_run)
    p = sub.add_parser("backfill")
    p.add_argument("--days", type=int, default=90)
    p.set_defaults(func=command_backfill)
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
