# -*- coding: utf-8 -*-
import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from WindPy import w


TABLE_NAME = "st_stock_list"
KEY_COLUMNS = ["exchange_id", "instrument_id"]
DATA_COLUMNS = ["exchange_id", "instrument_id", "instrument_name"]
DEFAULT_OUTPUT_DIR = "C:/quant_data/DolphinDB"


def parse_trade_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y%m%d").strftime("%Y%m%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid trade date {value!r}; expected YYYYMMDD"
        ) from exc


def get_mysql_config() -> Dict[str, object]:
    return {
        "host": os.environ.get("ST_MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("ST_MYSQL_PORT", "3306")),
        "user": os.environ.get("ST_MYSQL_USER", "root"),
        "password": os.environ.get("ST_MYSQL_PASSWORD", "Yc89Mysql"),
        "database": os.environ.get("ST_MYSQL_DATABASE", "stock_app"),
        "charset": os.environ.get("ST_MYSQL_CHARSET", "utf8mb4"),
    }


def start_wind(timeout: int) -> None:
    result = w.start()
    if getattr(result, "ErrorCode", 0) != 0:
        raise RuntimeError(f"Wind start failed with error code {result.ErrorCode}")

    deadline = time.monotonic() + timeout
    while not w.isconnected():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Wind connection timed out after {timeout} seconds")
        print("st_stock_list | wind | waiting for connection...")
        time.sleep(1)


def resolve_latest_completed_trade_date(explicit_trade_date: str = None) -> Tuple[str, bool]:
    if explicit_trade_date:
        return explicit_trade_date, False

    yesterday = datetime.now().date() - timedelta(days=1)
    search_start = yesterday - timedelta(days=31)
    result = w.tdays(
        search_start.strftime("%Y-%m-%d"),
        yesterday.strftime("%Y-%m-%d"),
        "",
    )
    if result.ErrorCode != 0:
        raise RuntimeError(
            f"Wind trading-calendar query failed with error code {result.ErrorCode}"
        )
    if not result.Data or not result.Data[0]:
        raise RuntimeError(
            f"Wind returned no completed trade date through {yesterday:%Y%m%d}"
        )

    latest = pd.Timestamp(result.Data[0][-1]).strftime("%Y%m%d")
    print(f"st_stock_list | calendar | latest completed trade date={latest}")
    return latest, True


def fetch_st_stock_snapshot(trade_date: str) -> pd.DataFrame:
    sector_ids = (
        "a001050100000000",
        "a001050200000000",
    )
    frames = []

    for sector_id in sector_ids:
        result = w.wset(
            "sectorconstituent",
            date=trade_date,
            sectorid=sector_id,
            field="wind_code,sec_name",
        )
        if result.ErrorCode != 0:
            raise RuntimeError(
                f"Wind wset failed for sector {sector_id}, error code {result.ErrorCode}"
            )
        if len(result.Data) < 2:
            raise RuntimeError(f"Wind returned an invalid response for sector {sector_id}")

        frames.append(
            pd.DataFrame(
                {
                    "wind_code": result.Data[0],
                    "instrument_name": result.Data[1],
                }
            )
        )

    source = pd.concat(frames, ignore_index=True)
    if source.empty:
        raise RuntimeError(
            f"Wind returned an empty ST stock list for {trade_date}; synchronization stopped"
        )

    split_codes = source["wind_code"].astype(str).str.rsplit(".", n=1, expand=True)
    if split_codes.shape[1] != 2 or split_codes.isna().any(axis=None):
        invalid_codes = source.loc[
            ~source["wind_code"].astype(str).str.contains(r"\.", regex=True),
            "wind_code",
        ].tolist()
        raise ValueError(f"Invalid Wind codes: {invalid_codes[:10]}")

    snapshot = pd.DataFrame(
        {
            "exchange_id": split_codes[1].str.strip().str.upper(),
            "instrument_id": split_codes[0].str.strip(),
            "instrument_name": source["instrument_name"].astype(str).str.strip(),
        }
    )
    snapshot = (
        snapshot.drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )

    if snapshot.empty:
        raise RuntimeError(
            f"No valid ST stock records remained after parsing for {trade_date}"
        )
    if snapshot[KEY_COLUMNS].isna().any(axis=None):
        raise ValueError("ST stock key columns contain null values")
    if snapshot.duplicated(KEY_COLUMNS).any():
        raise ValueError("ST stock snapshot contains duplicate keys")

    return snapshot[DATA_COLUMNS]


def build_output_path(
    output_dir: str,
    category: str,
    trade_date: str,
    suffix: str = "",
) -> Path:
    filename = f"{trade_date}{suffix}.parquet"
    return Path(output_dir) / "st_stock_list" / category / trade_date[:4] / filename


def save_parquet(
    frame: pd.DataFrame,
    path: Path,
    trade_date: str,
    force: bool,
) -> None:
    if path.exists() and not force:
        raise FileExistsError(
            f"Output already exists: {path}; pass --force to overwrite it"
        )

    output = frame.copy()
    output.insert(0, "snapshot_date", pd.to_datetime(trade_date, format="%Y%m%d"))
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(path, index=False, compression="snappy")
    print(f"st_stock_list | parquet | rows={len(output)}, file={path}")


def create_mysql_engine():
    config = get_mysql_config()
    url = URL.create(
        drivername="mysql+pymysql",
        username=str(config["user"]),
        password=str(config["password"]),
        host=str(config["host"]),
        port=int(config["port"]),
        database=str(config["database"]),
        query={"charset": str(config["charset"])},
    )
    return create_engine(url, pool_pre_ping=True)


def load_database_snapshot(connection) -> pd.DataFrame:
    query = text(
        f"SELECT exchange_id, instrument_id, instrument_name FROM {TABLE_NAME}"
    )
    database = pd.read_sql(query, connection)
    if database.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)

    for column in DATA_COLUMNS:
        database[column] = database[column].fillna("").astype(str).str.strip()
    database["exchange_id"] = database["exchange_id"].str.upper()
    return (
        database.drop_duplicates(subset=KEY_COLUMNS, keep="last")
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )


def calculate_diff(
    wind_snapshot: pd.DataFrame,
    database_snapshot: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    wind_indexed = wind_snapshot.set_index(KEY_COLUMNS, drop=False)
    database_indexed = database_snapshot.set_index(KEY_COLUMNS, drop=False)

    insert_keys = wind_indexed.index.difference(database_indexed.index)
    delete_keys = database_indexed.index.difference(wind_indexed.index)

    to_insert = wind_indexed.loc[insert_keys, DATA_COLUMNS].reset_index(drop=True)
    to_delete = database_indexed.loc[delete_keys, DATA_COLUMNS].reset_index(drop=True)
    return to_insert, to_delete


def validate_write_safety(
    trade_date: str,
    database_count: int,
    delete_count: int,
    allow_historical_write: bool,
    automatically_resolved_date: bool,
    allow_large_delete: bool,
    max_delete_count: int,
    max_delete_ratio: float,
) -> None:
    allowed_dates = {
        datetime.now().strftime("%Y%m%d"),
        (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
    }
    if (
        trade_date not in allowed_dates
        and not automatically_resolved_date
        and not allow_historical_write
    ):
        raise RuntimeError(
            "Historical snapshot write is blocked because the MySQL table stores the "
            "current effective list; pass --allow-historical-write only after review"
        )

    delete_ratio = delete_count / database_count if database_count else 0.0
    if (
        not allow_large_delete
        and (delete_count > max_delete_count or delete_ratio > max_delete_ratio)
    ):
        raise RuntimeError(
            "Delete safety threshold exceeded: "
            f"count={delete_count}, ratio={delete_ratio:.2%}; "
            "review diff parquet before passing --allow-large-delete"
        )


def apply_database_diff(connection, to_insert: pd.DataFrame, to_delete: pd.DataFrame) -> None:
    if not to_delete.empty:
        delete_sql = text(
            f"""
            DELETE FROM {TABLE_NAME}
            WHERE exchange_id = :exchange_id
              AND instrument_id = :instrument_id
            """
        )
        connection.execute(
            delete_sql,
            to_delete[KEY_COLUMNS].to_dict(orient="records"),
        )

    if not to_insert.empty:
        to_insert[DATA_COLUMNS].to_sql(
            name=TABLE_NAME,
            con=connection,
            if_exists="append",
            index=False,
            method="multi",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch the Wind ST stock snapshot and optionally synchronize MySQL."
    )
    parser.add_argument(
        "--trade-date",
        type=parse_trade_date,
        default=None,
        help=(
            "Snapshot date in YYYYMMDD format. When omitted, Wind selects the latest "
            "completed trade date through yesterday."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Base directory for snapshot and diff parquet files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Development mode marker. Database writes remain disabled.",
    )
    parser.add_argument(
        "--compare-db",
        action="store_true",
        help="Read MySQL and save insert/delete diff parquet without changing data.",
    )
    parser.add_argument(
        "--enable-write",
        action="store_true",
        help="Apply the reviewed insert/delete diff to MySQL in one transaction.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing parquet files.",
    )
    parser.add_argument("--wind-timeout", type=int, default=60)
    parser.add_argument("--max-delete-count", type=int, default=100)
    parser.add_argument("--max-delete-ratio", type=float, default=0.20)
    parser.add_argument("--allow-large-delete", action="store_true")
    parser.add_argument("--allow-historical-write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.dry_run and args.enable_write:
        raise ValueError("--dry-run and --enable-write cannot be used together")
    if args.wind_timeout <= 0:
        raise ValueError("--wind-timeout must be greater than zero")
    if args.max_delete_count < 0:
        raise ValueError("--max-delete-count cannot be negative")
    if not 0 <= args.max_delete_ratio <= 1:
        raise ValueError("--max-delete-ratio must be between 0 and 1")

    write_enabled = bool(args.enable_write)
    compare_database = bool(args.compare_db or write_enabled)
    mode = "production write enabled" if write_enabled else "development dry-run"
    print(f"st_stock_list | mode | {mode}, database writes {'enabled' if write_enabled else 'disabled'}")

    start_wind(args.wind_timeout)
    trade_date, automatically_resolved_date = resolve_latest_completed_trade_date(
        args.trade_date
    )
    print(
        "st_stock_list | plan | "
        f"trade_date={trade_date}, automatic_date={automatically_resolved_date}, "
        f"compare_db={compare_database}, output_dir={args.output_dir}"
    )

    snapshot_path = build_output_path(args.output_dir, "snapshot", trade_date)
    if (
        automatically_resolved_date
        and snapshot_path.exists()
        and not args.force
        and not compare_database
    ):
        print(
            "st_stock_list | skip | latest completed trade date already processed: "
            f"{trade_date}, file={snapshot_path}"
        )
        return 0

    snapshot = fetch_st_stock_snapshot(trade_date)
    print(f"st_stock_list | wind | fetched {len(snapshot)} unique ST stocks")

    save_parquet(
        snapshot,
        snapshot_path,
        trade_date,
        args.force or (automatically_resolved_date and snapshot_path.exists()),
    )

    if not compare_database:
        print("st_stock_list | diff | skipped; pass --compare-db to compare with MySQL")
        return 0

    engine = create_mysql_engine()
    try:
        with engine.connect() as connection:
            database_snapshot = load_database_snapshot(connection)
            to_insert, to_delete = calculate_diff(snapshot, database_snapshot)

        print(
            "st_stock_list | diff | "
            f"database={len(database_snapshot)}, wind={len(snapshot)}, "
            f"insert={len(to_insert)}, delete={len(to_delete)}"
        )
        insert_path = build_output_path(
            args.output_dir, "diff", trade_date, "_insert"
        )
        delete_path = build_output_path(
            args.output_dir, "diff", trade_date, "_delete"
        )
        diff_force = args.force or automatically_resolved_date
        save_parquet(to_insert, insert_path, trade_date, diff_force)
        save_parquet(to_delete, delete_path, trade_date, diff_force)

        if not write_enabled:
            print("st_stock_list | mysql | dry-run complete, no data changed")
            return 0

        validate_write_safety(
            trade_date=trade_date,
            database_count=len(database_snapshot),
            delete_count=len(to_delete),
            allow_historical_write=args.allow_historical_write,
            automatically_resolved_date=automatically_resolved_date,
            allow_large_delete=args.allow_large_delete,
            max_delete_count=args.max_delete_count,
            max_delete_ratio=args.max_delete_ratio,
        )
        with engine.begin() as connection:
            apply_database_diff(connection, to_insert, to_delete)
        print(
            "st_stock_list | mysql | synchronization committed, "
            f"inserted={len(to_insert)}, deleted={len(to_delete)}"
        )
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"st_stock_list | failed | {exc}", file=sys.stderr)
        raise
