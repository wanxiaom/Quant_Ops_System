# -*- coding: utf-8 -*-
from WindPy import w
import pandas as pd
import os
import time
import argparse
import dolphindb as ddb

import sys
from pathlib import Path
from datetime import datetime, timedelta
sys.path.insert(0, str(Path(__file__).parent.parent))
from wind_utils import wind_code
from my_utils import trade_date_pre
from ddb_test_utils import write_table_by_date

def start_wind() -> None:
    w.start()
    while not w.isconnected():
        print("Waiting for Wind connection...")
        time.sleep(1)

database = os.getenv("DB_NAME", "example_db")
table = 'consensus_rolling'
field = ""
field_mapping = {f.upper(): f for f in field.split(',')}


def download(trade_date: str, output_dir: str, force: bool = False):
    year = trade_date[:4]
    output_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")

    if os.path.exists(output_path) and not force:
        print(f"{database}.{table} | download | skipping (exists): {trade_date}")
        return True

    print(f"{database}.{table} | download | fetching data for: {trade_date}")
    try:
        codes = wind_code.get_wind_code(trade_date)
        if not codes:
            print(f"{database}.{table} | download | no valid codes: {trade_date}")
            return

        # 调用 Wind 批量获取当天所有股票的字段
        res = w.wss(
            codes=codes,
            fields=field,
            options=f"unit=1;tradeDate={trade_date};year=2025"
        )
        time.sleep(0.39)

        if res.ErrorCode != 0 or not hasattr(res, 'Data') or not res.Data:
            print(f"{database}.{table} | download | no valid data: {trade_date}")
            return

        # 构建 DataFrame
        df = pd.DataFrame(
            data=list(zip(*res.Data)),
            columns=res.Fields
        )

        # 使用你定义的映射重命名列（保留 FY1/YOY/1w 等格式）
        df.columns = [field_mapping.get(col.upper()) for col in df.columns]

        df['code'] = res.Codes  # wss 返回的 Codes 顺序与 Data 一致
        df['code'] = df['code'].astype('string')
        df['date'] = pd.to_datetime(trade_date, format='%Y%m%d')

        factor_cols = [col for col in df.columns if col not in ['code', 'date']]
        df = df[['code', 'date'] + factor_cols]
        df = df.dropna(subset=factor_cols, how='all')  # 只有当所有 factor_cols 都是 NaN 时才删除

        if df.empty:
            print(f"{database}.{table} | download | cleaned data is empty: {trade_date}")
            return

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False, compression='snappy')
        print(f"{database}.{table} | download | successfully saved {trade_date} with {len(df)} rows")
        return True

    except Exception as e:
        print(f"{database}.{table} | download | failed with error: {e}")
        raise


def upload(
    trade_date: str,
    output_dir: str,
    dry_run: bool = False,
    enable_write: bool = False,
    test_write: bool = False,
):
    year = trade_date[:4]
    input_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Parquet file not found: {input_path}")

    df = pd.read_parquet(input_path)
    if df.empty:
        print(f"{database}.{table} | upload | no valid data: {trade_date}")
        return
    if dry_run or not enable_write:
        target = f"{table}_test" if test_write else table
        print(
            f"DRY-RUN: {database}.{target} | upload | "
            f"would have uploaded {len(df)} rows for {trade_date}"
        )
        return

    write_table_by_date(
        df,
        f"dfs://{database}",
        table,
        trade_date,
        test_write=test_write,
        date_column="date",
        key_columns=("code", "date"),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SOP-compliant Downloader for Wind Consensus Rolling Forecast.")
    parser.add_argument('--trade-date', type=str, help='The trade date to process, format YYYYMMDD. Defaults to today.')
    parser.add_argument('--output-dir', type=str, default='C:/quant_data/DolphinDB', help="Base directory for output parquet files.")
    parser.add_argument('--force', action='store_true', help="Force re-download even if file exists.")
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Skip DolphinDB writes.",
    )
    parser.add_argument('--enable-write', action='store_true', help='Explicitly enable DolphinDB writes.')
    parser.add_argument('--test-write', action='store_true', help='Write only to the guarded <table>_test table.')
    parser.add_argument('--upload-only', action='store_true', help='Skip Wind download and upload an existing parquet file.')
    
    args = parser.parse_args()

    trade_date_to_run = args.trade_date if args.trade_date else datetime.now().strftime('%Y%m%d')

    if args.upload_only:
        if not args.trade_date:
            parser.error('--upload-only requires --trade-date')
        upload(
            trade_date_to_run, args.output_dir,
            dry_run=args.dry_run, enable_write=args.enable_write,
            test_write=args.test_write,
        )
        raise SystemExit(0)

    if not trade_date_pre.is_trade_date(trade_date_to_run):
        print(f"{database}.{table} | skip | {trade_date_to_run} is not a trade date")
        raise SystemExit(0)

    start_wind()
    print(f"\n--- Processing {database}.{table} for {trade_date_to_run} ---")
    if download(trade_date_to_run, args.output_dir, force=args.force):
        upload(
            trade_date_to_run, args.output_dir,
            dry_run=args.dry_run, enable_write=args.enable_write,
            test_write=args.test_write,
        )
