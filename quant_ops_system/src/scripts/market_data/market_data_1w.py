import rqdatac
import os
import sys
import pandas as pd
import datetime
import argparse
import dolphindb as ddb
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from my_utils import rq_index
from my_utils import rq_stock
from my_utils import trade_date_pre
from ddb_test_utils import write_test_table_by_date
from runtime import init_rqdatac

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = str(PROJECT_ROOT / "F:/DolphinDB")

# === 配置 ===
# RQData is initialized lazily only when downloading.
selected_columns = ['order_book_id', 'date', 'open', 'high', 'low', 'close', 'volume', 'total_turnover']

def fetch_weekly_data_by_natural_week(start_date: str, end_date: str, database: str, table: str, output_dir: str, dry_run: bool, enable_write: bool, test_write: bool):
    """
    按自然周 [start_date, end_date] 拉取周线数据。
    使用你的 utils 获取标的列表和 code 映射。
    注意：虽然输入是自然周，但 utils 需要一个具体日期来获取当日有效标的。
    我们使用 end_date 作为参考日（即该周最后一个日历日）。
    """
    print(f"  Fetching {table} weekly data for natural week: {start_date} ~ {end_date}")
    try:
        # Step 0: 在 [start_date, end_date] 范围内，从 end_date 开始倒序找第一个交易日作为 ref_date
        ref_date = None

        # 将字符串转为 datetime 便于遍历
        start_dt = datetime.datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.datetime.strptime(end_date, '%Y%m%d')

        # 从 end_dt 到 start_dt 倒序遍历每一天
        current = end_dt
        while current >= start_dt:
            current_str = current.strftime('%Y%m%d')
            if trade_date_pre.is_trade_date(current_str):
                ref_date = current_str
                break
            current -= datetime.timedelta(days=1)

        if ref_date is None:
            print(f"    No trading day found in natural week {start_date}~{end_date}, skipping.")
            return

        print(f"    Using reference date (last trading day in week): {ref_date}")

        # Step 1: 使用你的 utils 获取标的和映射
        if table == 'stock':
            instruments = rq_stock.get_code_list(ref_date)
            instruments_dict = rq_stock.get_code_dict(ref_date)
        elif table == 'index':
            instruments = rq_index.get_index_list(ref_date)
            instruments_dict = rq_index.get_index_dict(ref_date)
        else:
            raise ValueError(f"Unsupported table: {table}")

        if not instruments or not instruments_dict:
            print(f"    No valid {table} instruments on {ref_date}, skipping.")
            return

        # Step 2: 拉取周线数据（覆盖该自然周）
        df_price = rqdatac.get_price(
            instruments,
            start_date=start_date,
            end_date=end_date,
            frequency='1w',
            fields=None,
            adjust_type='none',
            skip_suspended=False,
            market='cn',
            expect_df=True
        )

        if df_price.empty:
            print(f"    No weekly data returned (no trading days covered).")
            return

        # 处理返回的数据
        df_price = df_price.reset_index()
        unique_dates = df_price['date'].drop_duplicates().sort_values()

        for real_date in unique_dates:
            real_date_str = real_date.strftime('%Y%m%d')
            year = real_date_str[:4]
            output_path = os.path.join(output_dir, database, table, year, f"{real_date_str}.parquet")

            if os.path.exists(output_path):
                print(f"    Reusing existing parquet: {real_date_str}")
                update_weekly_to_dolphindb(
                    real_date_str, database, table, output_dir,
                    dry_run, enable_write, test_write
                )
                continue

            group = df_price[df_price['date'] == real_date].copy()
            df_selected = group[selected_columns].copy()
            df_clean = df_selected.dropna(subset=selected_columns, how='all').copy()

            if df_clean.empty:
                print(f"    Cleaned data empty for {real_date_str}, skipping.")
                continue

            # 使用你的 code_dict 映射
            df_clean['code'] = df_clean['order_book_id'].map(instruments_dict)
            df_clean = df_clean.drop(columns=['order_book_id'])

            cols = ['code'] + [col for col in df_clean.columns if col != 'code']
            df_clean = df_clean[cols]

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_clean.to_parquet(output_path, index=False, compression='snappy')
            print(f"    Saved {output_path} with {len(df_clean)} records.")

            update_weekly_to_dolphindb(real_date_str, database, table, output_dir, dry_run, enable_write, test_write)

    except Exception as e:
        print(f"    Failed to fetch {table} for week {start_date}-{end_date}: {str(e)}")
        raise


def update_weekly_to_dolphindb(
    trade_date: str,
    database: str,
    table: str,
    output_dir: str,
    dry_run: bool,
    enable_write: bool,
    test_write: bool,
):
    year = trade_date[:4]
    parquet_path = os.path.join(output_dir, database, table, year, f"{trade_date}.parquet")
    if not os.path.exists(parquet_path):
        print(f"    Parquet not found: {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    if df.empty:
        print("    Empty DataFrame, skipping.")
        return
    if dry_run or not enable_write:
        print(f"    DRY-RUN: {database}.{table} {trade_date}, rows={len(df)}")
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
        session.upload({"tmp_tb": df})
        session.run(f"""
            target = loadTable("dfs://{database}", "{table}")
            targetDate = temporalParse("{trade_date}", "yyyyMMdd")
            delete from target where date = targetDate
            tableInsert(target, tmp_tb)
        """)
    finally:
        session.close()
    print(f"    Appended to DolphinDB: {database}.{table} {trade_date}, rows={len(df)}")


def upload_existing_weekly_parquets(
    start_date: str,
    end_date: str,
    database: str,
    tables: list[str],
    output_dir: str,
    dry_run: bool,
    enable_write: bool,
    test_write: bool,
) -> int:
    """Upload existing weekly parquet files without initializing RQData."""
    uploaded = 0
    for table_name in tables:
        table_root = Path(output_dir) / database / table_name
        files = sorted(table_root.glob("*/*.parquet")) if table_root.exists() else []
        selected = [
            file for file in files
            if file.stem.isdigit() and start_date <= file.stem <= end_date
        ]
        if not selected:
            print(
                f"No existing parquet files for {database}.{table_name} "
                f"in range {start_date}-{end_date}: {table_root}"
            )
            continue
        for parquet_file in selected:
            update_weekly_to_dolphindb(
                parquet_file.stem, database, table_name, output_dir,
                dry_run, enable_write, test_write
            )
            uploaded += 1
    if uploaded == 0:
        raise FileNotFoundError(
            f"No weekly parquet files found in {output_dir} for {start_date}-{end_date}"
        )
    return uploaded


def upload_parquet_folder_to_dolphindb(
    folder_path: str,
    database: str,
    table: str,
    recursive: bool = False,
    host: str = "127.0.0.1",
    port: int = 8902,
    user: str = "your_username",
    password: str = "your_password",
    enable_write: bool = False,
    test_write: bool = False,
):
    """
    将指定文件夹中的所有 .parquet 文件导入 DolphinDB 分布式表。

    参数:
        folder_path (str): Parquet 文件所在文件夹路径
        database (str): DolphinDB 数据库名（如 "dfs://stock"）
        table (str): 表名
        recursive (bool): 是否递归子目录，默认 False
        host, port, user, password: DolphinDB 连接参数
    """
    if not enable_write:
        print("DRY-RUN: bulk weekly DolphinDB upload is disabled without enable_write")
        return

    folder = Path(folder_path)
    if not folder.exists():
        print(f"❌ Folder not found: {folder_path}")
        return

    # 获取所有 .parquet 文件
    pattern = "**/*.parquet" if recursive else "*.parquet"
    parquet_files = list(folder.glob(pattern))

    if not parquet_files:
        print(f"⚠️ No .parquet files found in {folder_path}")
        return

    print(f"📁 Found {len(parquet_files)} Parquet file(s) in {folder_path}")

    # 建立 DolphinDB 会话（复用连接，提升效率）
    s = ddb.session()
    try:
        s.connect(host, port, user, password)
        db_path = f"dfs://{database}"
        s.run(f"""
            db = database("{db_path}")
            pt = db.loadTable("{table}")
        """)
        print(f"✅ Connected to DolphinDB: {db_path}.{table}")

        success_count = 0
        for parquet_file in sorted(parquet_files):  # 按文件名排序，保证顺序
            try:
                print(f"  → Processing: {parquet_file.name}")
                df = pd.read_parquet(parquet_file)

                if df.empty:
                    print(f"    ⚠️ Skipping empty file: {parquet_file.name}")
                    continue

                if test_write:
                    trade_date = parquet_file.stem
                    write_test_table_by_date(
                        df, db_path, table, trade_date, key_columns=("code", "date")
                    )
                else:
                    s.upload({'tmp_tb': df})
                    s.run("pt.append!(tmp_tb)")
                success_count += 1
                print(f"    ✅ Appended {len(df)} rows from {parquet_file.name}")

            except Exception as e:
                print(f"    ❌ Failed to process {parquet_file.name}: {e}")
                continue  # 继续处理下一个文件

        print(f"\n🎉 Completed: {success_count}/{len(parquet_files)} files imported successfully.")

    except Exception as e:
        print(f"💥 DolphinDB connection or table load failed: {e}")
    finally:
        s.close()


# === 主入口：按自然周全量拉取 ===
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SOP-compliant Market Data Downloader for Weekly Data.")
    parser.add_argument('--start-date', type=str, help='Start date (YYYYMMDD). Defaults to 7 days ago.')
    parser.add_argument('--end-date', type=str, help='End date (YYYYMMDD). Defaults to today.')
    parser.add_argument('--tables', nargs='+', default=['stock', 'index'], help="List of tables to download, e.g., stock index etf.")
    parser.add_argument('--output-dir', type=str, default=DEFAULT_OUTPUT_DIR, help="Base directory for output parquet files.")
    parser.add_argument('--dry-run', action='store_true', help="If set, skips the actual database upload step.")
    parser.add_argument('--enable-write', action='store_true', help="Explicitly enable DolphinDB writes.")
    parser.add_argument('--test-write', action='store_true', help="Write only to stock_test/index_test.")
    parser.add_argument('--upload-only', action='store_true', help="Skip RQData and upload existing weekly parquet files in the date range.")
    
    args = parser.parse_args()

    database = os.getenv("DB_NAME", "example_db")
    tables = args.tables

    # 全量时间范围（自然日）
    if args.end_date:
        end_date = datetime.datetime.strptime(args.end_date, '%Y%m%d')
    else:
        end_date = datetime.datetime.today()

    if args.start_date:
        start_date = datetime.datetime.strptime(args.start_date, '%Y%m%d')
    else:
        # 默认回溯一周，确保能覆盖到上一个完整的自然周
        start_date = end_date - datetime.timedelta(days=7)
        
    start_value = start_date.strftime('%Y%m%d')
    end_value = end_date.strftime('%Y%m%d')
    if args.upload_only:
        count = upload_existing_weekly_parquets(
            start_value, end_value, database, tables, args.output_dir,
            args.dry_run, args.enable_write, args.test_write
        )
        print(f"Weekly upload-only completed: {count} parquet file(s).")
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

    print(f"Starting full backfill by natural weeks from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

    current = start_date
    week_count = 0

    while current <= end_date:
        # 自然周：周一到周日
        monday = current - datetime.timedelta(days=current.weekday())  # 周一
        sunday = monday + datetime.timedelta(days=6)                   # 周日

        # 转为 YYYYMMDD 字符串（RQData 接受 str 或 datetime）
        start_str = monday.strftime('%Y%m%d')
        end_str = min(sunday, end_date).strftime('%Y%m%d')  # 最后一周可能截断

        week_count += 1
        print(f"\n[Week {week_count}] Natural week: {start_str} ~ {end_str}")

        for table in tables:
            fetch_weekly_data_by_natural_week(start_str, end_str, database, table, args.output_dir, args.dry_run, args.enable_write, args.test_write)

        # 下一周
        current = sunday + datetime.timedelta(days=1)

    print("\n✅ Full weekly data backfill completed!")
