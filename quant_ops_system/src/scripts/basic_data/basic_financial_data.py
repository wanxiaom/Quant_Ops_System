# -*- coding: utf-8 -*-
import rqdatac
import dolphindb as ddb
import pandas as pd
import os
import argparse

import sys
from pathlib import Path
from datetime import datetime, timedelta
sys.path.insert(0, str(Path(__file__).parent.parent))
from my_utils import rq_stock
from my_utils import trade_date_pre
from ddb_test_utils import write_test_table_by_date

# 资产负债表 156
balance_sheet_mrq0 = [

]
balance_sheet_ttm0 = [

]
balance_sheet_lyr0 = [

]

# 现金流量表 71
cashflow_statement_mrq0 = [

]
cashflow_statement_ttm0 = [

]

# 利润表 91
income_statement_mrq0 = [

]

import time
for _i in range(5):
    try:
        rqdatac.init(os.getenv("RQ_USERNAME", "your_username"), os.getenv("RQ_PASSWORD", "your_password"), os.getenv("RQ_LICENSE_KEY", "your_rq_license_key"))
        break
    except Exception as e:
        if "login machine num exceeds" in str(e) and _i < 4:
            print(f"RQData quota exceeded, waiting 5 seconds for previous connection to drop... ({_i+1}/5)")
            time.sleep(5)
        else:
            raise

infos = [
    ('income_statement', 'mrq0', income_statement_mrq0),
    ('income_statement', 'ttm0', income_statement_ttm0),
    ('income_statement', 'lyr0', income_statement_lyr0),
    ('balance_sheet', 'mrq0', balance_sheet_mrq0),
    ('balance_sheet', 'ttm0', balance_sheet_ttm0),
    ('balance_sheet', 'lyr0', balance_sheet_lyr0),
    ('cashflow_statement', 'mrq0', cashflow_statement_mrq0),
    ('cashflow_statement', 'ttm0', cashflow_statement_ttm0),
    ('cashflow_statement', 'lyr0', cashflow_statement_lyr0),
]


def download(trade_date: str, info: tuple, output_dir: str, force: bool = False):
    database, table, metrics = info
    year = trade_date[:4]
    output_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")

    if os.path.exists(output_path) and not force:
        print(f"{database}.{table} | download | skipping (exists): {trade_date}")
        return

    print(f"{database}.{table} | download | fetching data for: {trade_date}")
    try:
        codes = rq_stock.get_code_list(trade_date)
        code_dict = rq_stock.get_code_dict(trade_date)
        if not codes or not code_dict:
            print(f"{database}.{table} | download | no valid codes: {trade_date}")
            return

        df = rqdatac.get_factor(codes, metrics, start_date=trade_date, end_date=trade_date)
        if df.empty:
            print(f"{database}.{table} | download | no valid data: {trade_date}")
            return

        # 重置索引，得到 date, order_book_id, field1, field2...
        df = df.reset_index()
        df_clean = df.dropna(subset=metrics, how='all').copy()
        df_clean['code'] = df_clean['order_book_id'].map(code_dict)
        df_clean = df_clean.drop(columns=['order_book_id'])

        cols = ['code', 'date'] + [col for col in df_clean.columns if col not in ['code', 'date']]
        df_clean = df_clean[cols]

        if df_clean.empty:
            print(f"{database}.{table} | download | cleaned data is empty: {trade_date}")
            return

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_clean.to_parquet(output_path, index=False, compression='snappy')
        print(f"{database}.{table} | download | successfully saved {trade_date} with {len(df_clean)} rows")
        return True

    except Exception as e:
        print(f"{database}.{table} | download | failed with error: {e}")


def upload(
    trade_date: str,
    info: tuple,
    output_dir: str,
    dry_run: bool = False,
    enable_write: bool = False,
    test_write: bool = False,
):
    database, table, metrics = info
    year = trade_date[:4]
    input_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")
    if not os.path.exists(input_path):
        print(f"{database}.{table} | upload | file not found: {trade_date}")
        return

    df = pd.read_parquet(input_path)
    if df.empty:
        print(f"{database}.{table} | upload | no valid data: {trade_date}")
        return
    if dry_run or not enable_write:
        print(f"DRY-RUN: {database}.{table} | upload | rows={len(df)}, date={trade_date}")
        return
    if test_write:
        write_test_table_by_date(
            df, f"dfs://{database}", table, trade_date, key_columns=("code", "date")
        )
        return

    session = ddb.session()
    try:
        session.connect(
            os.getenv("DOLPHINDB_HOST", "${DOLPHINDB_HOST}"),
            int(os.getenv("DOLPHINDB_PORT", "${DOLPHINDB_PORT}")),
            os.getenv("DB_USERNAME", "your_username"),
            os.getenv("DB_PASSWORD", "your_password"),
        )
        session.upload({"tmp": df})
        session.run(f"""
            target = loadTable("dfs://{database}", "{table}")
            targetDate = temporalParse("{trade_date}", "yyyyMMdd")
            delete from target where date = targetDate
            tableInsert(target, tmp)
        """)
    finally:
        session.close()
    print(f"{database}.{table} | upload | successfully saved {trade_date} with {len(df)} rows")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SOP-compliant Downloader for Financial Data.")
    parser.add_argument('--trade-date', type=str, help='The trade date to process, format YYYYMMDD. Defaults to today.')
    parser.add_argument('--output-dir', type=str, default='/F:/DolphinDB', help="Base directory for output parquet files.")
    parser.add_argument('--force', action='store_true', help="Force re-download even if file exists.")
    parser.add_argument('--dry-run', action='store_true', help="If set, skips the actual database upload step.")
    parser.add_argument('--enable-write', action='store_true', help="Explicitly enable DolphinDB writes.")
    parser.add_argument('--test-write', action='store_true', help="Write only to <table>_test.")
    
    args = parser.parse_args()

    trade_date_to_run = args.trade_date if args.trade_date else datetime.now().strftime('%Y%m%d')

    if not trade_date_pre.is_trade_date(trade_date_to_run):
        print(f"basic_financial_data | skip | {trade_date_to_run} is not a trade date")
        raise SystemExit(0)

    for info in infos:
        database, table, _ = info
        print(f"\n--- Processing {database}.{table} for {trade_date_to_run} ---")
        if download(trade_date_to_run, info, args.output_dir, force=args.force):
            upload(
                trade_date_to_run, info, args.output_dir,
                dry_run=args.dry_run, enable_write=args.enable_write,
                test_write=args.test_write,
            )
