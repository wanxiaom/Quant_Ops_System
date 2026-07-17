#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging
import os
import sys
from pathlib import Path

import dolphindb as ddb
import pandas as pd
import rqdatac as rq

SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from runtime import init_rqdatac
from ddb_test_utils import write_test_table_by_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

INDICES = {
   
}


def normalize_date(value: str) -> str:
    normalized = value.replace(".", "-").replace("/", "-")
    if len(normalized) == 8 and normalized.isdigit():
        normalized = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
    try:
        return datetime.datetime.strptime(normalized, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date {value!r}; expected YYYYMMDD or YYYY-MM-DD"
        ) from exc


def latest_completed_trade_date() -> str:
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=31)
    dates = rq.get_trading_dates(start_date, end_date)
    if len(dates) == 0:
        raise RuntimeError(f"No completed trade date found through {end_date}")
    result = pd.Timestamp(dates[-1]).strftime("%Y-%m-%d")
    logging.info("Latest completed trade date: %s", result)
    return result


def get_trading_dates(start_date: str, end_date: str) -> list[str]:
    if start_date > end_date:
        raise ValueError("start date must not be later than end date")
    return [pd.Timestamp(value).strftime("%Y-%m-%d") for value in rq.get_trading_dates(start_date, end_date)]


def fetch_index_components(trade_date: str, require_all: bool) -> pd.DataFrame:
    all_data = []
    missing_indices = []

    for order_book_id, (name, target_code) in INDICES.items():
        logging.info(
            "Fetching components and weights for %s (%s) on %s...",
            name,
            order_book_id,
            trade_date,
        )
        try:
            weights = rq.index_weights(order_book_id, date=trade_date)
            if weights is None or weights.empty:
                missing_indices.append(order_book_id)
                logging.warning("No data returned for %s on %s.", name, trade_date)
                continue

            frame = pd.DataFrame(
                {
                    "stock_code": weights.index.str.replace(
                        ".XSHG", ".SH", regex=False
                    ).str.replace(".XSHE", ".SZ", regex=False),
                    "weight": weights.values,
                }
            )
            frame["code"] = target_code
            frame["date"] = trade_date.replace("-", ".")
            all_data.append(frame[["code", "date", "stock_code", "weight"]])
            logging.info("Successfully fetched %d components for %s.", len(frame), name)
        except Exception as exc:
            missing_indices.append(order_book_id)
            logging.warning(
                "Skipping %s (%s) on %s because source data is unavailable: %s",
                name,
                order_book_id,
                trade_date,
                exc,
            )

    if require_all and missing_indices:
        raise RuntimeError(
            "Index component data is incomplete for "
            f"{trade_date}; missing indices: {', '.join(missing_indices)}"
        )
    if not all_data:
        raise RuntimeError(f"No index component data fetched for {trade_date}")

    result = pd.concat(all_data, ignore_index=True)
    if result.duplicated(["code", "date", "stock_code"]).any():
        raise RuntimeError(f"Duplicate index component keys found for {trade_date}")
    logging.info(
        "Fetched %d rows for %d/%d indices on %s.",
        len(result),
        len(all_data),
        len(INDICES),
        trade_date,
    )
    if missing_indices:
        logging.warning(
            "Missing %d/%d indices on %s: %s",
            len(missing_indices),
            len(INDICES),
            trade_date,
            ", ".join(missing_indices),
        )
    return result


def upload_to_dolphindb(trade_date: str, frame: pd.DataFrame, force: bool, test_write: bool = False) -> None:
    logging.info("Starting DolphinDB synchronization for %s...", trade_date)
    if test_write:
        write_test_table_by_date(
            frame,
            "dfs://index_info",
            "index_component",
            trade_date.replace("-", ""),
            key_columns=("code", "date", "stock_code"),
        )
        return
    session = ddb.session()
    try:
        session.connect(
            os.environ.get("DOLPHINDB_HOST", "${DOLPHINDB_HOST}"),
            int(os.environ.get("DOLPHINDB_PORT", "${DOLPHINDB_PORT}")),
            os.environ.get("DB_USERNAME", "your_username"),
            os.environ.get("DB_PASSWORD", "your_password"),
        )
        session.upload({"tmp": frame})
        force_flag = "true" if force else "false"
        result = session.run(
            f"""
            tmp_parsed = select code,
                                temporalParse(date, "yyyy.MM.dd") as date,
                                stock_code,
                                weight
                         from tmp
            db_path = "dfs://index_info"
            target_date = temporalParse("{trade_date}", "yyyy-MM-dd")
            incoming_rows = size(tmp_parsed)
            existing_rows = 0
            action = "insert"

            if(existsDatabase(db_path)){{
                db = database(db_path)
            }} else {{
                db = database(db_path, VALUE, 2000.01.01..2035.12.31)
            }}

            if(existsTable(db_path, "index_component")){{
                pt = loadTable(db, "index_component")
                existing_rows = exec count(*) from pt where date = target_date
                if(existing_rows == incoming_rows && !{force_flag}){{
                    action = "skip"
                }} else {{
                    delete from pt where date = target_date
                    tableInsert(pt, tmp_parsed)
                    action = "replace"
                }}
            }} else {{
                pt = db.createPartitionedTable(
                    table=tmp_parsed,
                    tableName="index_component",
                    partitionColumns="date",
                    sortColumns=`code`stock_code`date
                )
                tableInsert(pt, tmp_parsed)
            }}

            table(action as action,
                  existing_rows as existing_rows,
                  incoming_rows as incoming_rows)
            """
        )
        if isinstance(result, pd.DataFrame) and not result.empty:
            action = result.iloc[0]["action"]
            existing = int(result.iloc[0]["existing_rows"])
            incoming = int(result.iloc[0]["incoming_rows"])
            logging.info(
                "DolphinDB synchronization complete: action=%s, existing=%d, incoming=%d",
                action,
                existing,
                incoming,
            )
        else:
            logging.info("DolphinDB synchronization complete for %s.", trade_date)
    finally:
        session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Ricequant index components and synchronize DolphinDB."
    )
    parser.add_argument(
        "legacy_dates",
        nargs="*",
        help="Legacy positional usage: [trade_date] or [start_date end_date].",
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--trade-date", type=normalize_date)
    date_group.add_argument("--start-date", type=normalize_date)
    parser.add_argument("--end-date", type=normalize_date)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace the target date even when the existing row count is complete.",
    )
    parser.add_argument(
        "--test-write",
        action="store_true",
        help="Write only to index_component_test.",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow missing indices. Kept for compatibility; partial mode is the default.",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="Fail the task when any configured index is unavailable.",
    )
    return parser.parse_args()


def resolve_dates(args: argparse.Namespace) -> tuple[list[str], bool]:
    if len(args.legacy_dates) > 2:
        raise SystemExit("At most two positional dates are supported")
    if args.legacy_dates and (args.trade_date or args.start_date or args.end_date):
        raise SystemExit("Positional dates cannot be combined with date options")
    if args.end_date and not args.start_date:
        raise SystemExit("--end-date requires --start-date")

    if len(args.legacy_dates) == 1:
        value = normalize_date(args.legacy_dates[0])
        return get_trading_dates(value, value), False
    if len(args.legacy_dates) == 2:
        start = normalize_date(args.legacy_dates[0])
        end = normalize_date(args.legacy_dates[1])
        return get_trading_dates(start, end), True
    if args.trade_date:
        return get_trading_dates(args.trade_date, args.trade_date), False
    if args.start_date:
        end = args.end_date or args.start_date
        return get_trading_dates(args.start_date, end), True
    return [latest_completed_trade_date()], False


def main() -> int:
    logging.info("--- Starting Ricequant Index Components Task ---")
    init_rqdatac(rq)
    args = parse_args()
    dates_to_run, is_range = resolve_dates(args)
    if not dates_to_run:
        logging.info("No trading date to process.")
        return 0

    require_all = args.require_all and not args.allow_partial
    for trade_date in dates_to_run:
        frame = fetch_index_components(trade_date, require_all=require_all)
        upload_to_dolphindb(trade_date, frame, force=args.force, test_write=args.test_write)

    logging.info("--- Task Completed Successfully ---")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        logging.exception("Index components task failed: %s", exc)
        raise
