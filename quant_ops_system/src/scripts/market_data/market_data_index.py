# -*- coding: utf-8 -*-
import rqdatac
import os
import pandas as pd
import dolphindb as ddb
import argparse

import sys
from pathlib import Path
from datetime import datetime, timedelta
sys.path.insert(0, str(Path(__file__).parent.parent))
from my_utils import trade_date_pre
from runtime import init_rqdatac
from ddb_test_utils import resolve_time_key_columns, write_test_table_by_date
from my_utils import rq_index

index_columns = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'total_turnover']
selected_columns = ['open', 'high', 'low', 'close', 'volume', 'total_turnover']
selected_columns_1d = ['prev_close', 'open', 'high', 'low', 'close', 'volume', 'total_turnover']

table = 'index'
TEST_TABLE = 'index_test'


def download(trade_date: str, database: str, output_dir: str, force: bool = False):
    frequency = database.split('_')[-1]
    is_daily = (frequency == '1d')
    columns_to_use = selected_columns_1d if is_daily else selected_columns

    year = trade_date[:4]
    # 使用传入的 output_dir
    output_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")

    if os.path.exists(output_path) and not force:
        print(f"{database}.{table} | download | skipping (exists): {trade_date}")
        return True

    print(f"{database}.{table} | download | fetching data for: {trade_date}")
    try:
        codes = rq_index.get_index_list(trade_date)
        index_dict = rq_index.get_index_dict(trade_date)
        if not codes or not index_dict:
            print(f"{database}.{table} | download | no valid codes: {trade_date}")
            return

        df_price = rqdatac.get_price(
            codes, start_date=trade_date, end_date=trade_date,
            frequency=frequency, fields=None, adjust_type='pre',
            skip_suspended=False, market='cn', expect_df=True
        )

        if df_price.empty:
            print(f"{database}.{table} | download | no valid data: {trade_date}")
            return

        df_selected = df_price[columns_to_use]
        df_clean = df_selected.reset_index()
        df_clean = df_clean.dropna(subset=columns_to_use, how='all').copy()
        df_clean['code'] = df_clean['order_book_id'].map(index_dict)
        df_clean = df_clean.drop(columns=['order_book_id'])

        cols = ['code'] + [col for col in df_clean.columns if col != 'code']
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
        sys.exit(1)


def upload(
    trade_date: str,
    database: str,
    output_dir: str,
    dry_run: bool = False,
    enable_write: bool = False,
    target_table: str = table,
    create_test_table: bool = False,
):
    year = trade_date[:4]
    # 使用传入的 output_dir
    input_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")

    if not os.path.exists(input_path):
        print(f"{database}.{table} |  upload  | file not found: {trade_date}")
        return

    try:
        df = pd.read_parquet(input_path)
        if df.empty:
            print(f"{database}.{table} | upload | no valid data: {trade_date}")
            return

        time_column, key_columns = resolve_time_key_columns(df)
        duplicate_count = int(df.duplicated(list(key_columns), keep="last").sum())
        if duplicate_count:
            df = df.drop_duplicates(list(key_columns), keep="last").reset_index(drop=True)
            print(
                f"{database}.{target_table} | upload | deduplicated "
                f"{duplicate_count} duplicate {'/'.join(key_columns)} rows; rows={len(df)}"
            )

        if dry_run or not enable_write:
            print(
                f"DRY-RUN: {database}.{target_table} | upload | "
                f"would have uploaded {len(df)} rows for {trade_date}"
            )
            return

        if target_table not in {table, TEST_TABLE}:
            raise ValueError(
                f"Unsupported target table: {target_table}. "
                f"Only {table} and {TEST_TABLE} are allowed."
            )
        if target_table == table and create_test_table:
            raise ValueError("--create-test-table can only be used with --target-table index_test")
        if target_table == TEST_TABLE:
            write_test_table_by_date(
                df,
                f"dfs://{database}",
                table,
                trade_date,
                date_column=time_column,
                key_columns=key_columns,
            )
            return

        s = ddb.session()
        s.connect(
            os.getenv("DOLPHINDB_HOST", "${DOLPHINDB_HOST}"),
            int(os.getenv("DOLPHINDB_PORT", "${DOLPHINDB_PORT}")),
            os.getenv("DB_USERNAME", "your_username"),
            os.getenv("DB_PASSWORD", "your_password"),
        )

        s.upload({'tmp': df})
        s.run(f"""
            db = database("dfs://{database}")
            sourceTable = "{table}"
            targetTable = "{target_table}"
            if(!existsTable("dfs://{database}", targetTable)){{
                if(targetTable != "{TEST_TABLE}" || !{str(create_test_table).lower()}){{
                    throw "Target table does not exist: " + targetTable
                }}
                sourceSchema = select top 0 * from loadTable(db, sourceTable)
                db.createPartitionedTable(
                    sourceSchema,
                    targetTable,
                    `date,
                    sortColumns=`code`date,
                    keepDuplicates=LAST
                )
            }}
            pt = loadTable(db, targetTable)
            targetDate = temporalParse("{trade_date}", "yyyyMMdd")
            delete from pt where {"date(datetime)" if time_column == "datetime" else "date"} = targetDate
            tableInsert(pt, tmp)
        """)
        row_count = s.run(f"""
            exec count(*) from loadTable("dfs://{database}", "{target_table}")
            where {"date(datetime)" if time_column == "datetime" else "date"} = temporalParse("{trade_date}", "yyyyMMdd")
        """)
        s.close()
        if int(row_count) != len(df):
            raise RuntimeError(
                f"row count mismatch: parquet={len(df)}, database={row_count}"
            )
        print(
            f"{database}.{target_table} | upload | successfully saved "
            f"{trade_date} with {row_count} rows"
        )

    except Exception as e:
        print(f"{database}.{target_table} | upload | failed with error: {e}")
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SOP-compliant Market Data Downloader for Indices.")
    parser.add_argument('--trade-date', type=str, help='The trade date to process, format YYYYMMDD. Defaults to today.')
    parser.add_argument('--frequencies', nargs='+', default=['1d', '60m', '30m', '15m', '5m', '1m'], help="List of frequencies to download, e.g., 1d 60m 1w.")
    parser.add_argument('--output-dir', type=str, default='/home/wanxm/ops_maintenance/quant_ops_system/F:/DolphinDB', help="Base directory for output parquet files.")
    parser.add_argument('--force', action='store_true', help="Force re-download even if file exists.")
    parser.add_argument('--dry-run', action='store_true', help="If set, skips the actual database upload step.")
    parser.add_argument('--enable-write', action='store_true', help="Explicitly enable DolphinDB writes.")
    parser.add_argument('--test-write', action='store_true', help="Write only to the guarded index_test table.")
    parser.add_argument('--upload-only', action='store_true', help="Skip RQData initialization/download and upload an existing parquet file.")
    parser.add_argument(
        '--target-table', choices=[table, TEST_TABLE], default=table,
        help="DolphinDB target table. Use index_test for database write testing.",
    )
    parser.add_argument(
        '--create-test-table', action='store_true',
        help="Create index_test from the index schema when it does not exist.",
    )
    
    args = parser.parse_args()

    if args.test_write:
        args.target_table = TEST_TABLE
        args.create_test_table = True

    trade_date_to_run = args.trade_date if args.trade_date else datetime.now().strftime('%Y%m%d')

    if args.upload_only:
        if not args.trade_date:
            parser.error('--upload-only requires --trade-date')
        for freq in args.frequencies:
            database_name = f"market_data_{freq}"
            print(f"\n--- Uploading existing {database_name}.{table} for {trade_date_to_run} ---")
            upload(
                trade_date_to_run, database_name, args.output_dir,
                dry_run=args.dry_run, enable_write=args.enable_write,
                target_table=args.target_table,
                create_test_table=args.create_test_table,
            )
        raise SystemExit(0)

    try:
        init_rqdatac(rqdatac)
    except Exception as exc:
        if "login machine num exceeds" in str(exc).lower():
            raise RuntimeError(
                "RQData login quota exceeded. Stop other RQData sessions or wait for "
                "the server to release them; use --upload-only when parquet already exists."
            ) from exc
        raise

    if not trade_date_pre.is_trade_date(trade_date_to_run):
        print(f"{table} | skip | {trade_date_to_run} is not a trade date")
        raise SystemExit(0)

    for freq in args.frequencies:
        database_name = f"market_data_{freq}"
        print(f"\n--- Processing {database_name}.{table} for {trade_date_to_run} ---")
        if download(trade_date_to_run, database_name, args.output_dir, force=args.force):
            upload(
                trade_date_to_run, database_name, args.output_dir,
                dry_run=args.dry_run, enable_write=args.enable_write,
                target_table=args.target_table,
                create_test_table=args.create_test_table,
            )
