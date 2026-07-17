import argparse
import os
import time
import warnings
from pathlib import Path
from typing import Union, List, Tuple, Any, Callable, Dict

import numpy as np
import pandas as pd
from tqdm import tqdm
from WindPy import w

import sys
SCRIPTS_ROOT = Path(__file__).resolve().parent.parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))
from ddb_test_utils import write_test_table_by_date


def start_wind() -> None:
    w.start()
    while not w.isconnected():
        print("Waiting for Wind connection...")
        time.sleep(1)

DATA_RULES = {
    # 指数行情数据
    "MKT": {
        "wsd_params": {
            "symbol": [  # 需要更新的标的代码
         
            ],
            "fields": "open,high,low,close,volume,pre_close,amt",  # wsd fields 字段
            "options": "unit=1",
            "columns_map": {  # columns rename
       
            }
        },
        "database": {  # 数据库参数
            "db_path": "dfs://market_data_1d_wind",
            "table_name": "index" 
        }
    },
    # 期权PCR数据
    "OPT": {
      
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d" 
        }
    },
    # 指数PE_TTM
    "PE": {
        "wsd_params": {
            "symbol": [
                "881001.WI", 
                "000300.SH", 
                "000905.SH", 
                "000906.SH", 
                "000852.SH"
            ],
            "fields": "pe_ttm", 
            "options": "",
            "columns_map": {
                "PE_TTM": "pe_ttm"
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d" 
        }
    },
    # 机构/大单/中单/小单 资金流入/流出额
    "CF_Int": {
        "wsd_params": {
            "symbol": [
                "881001.WI", 
                "000300.SH", 
                "000905.SH", 
                "000906.SH", 
                "000852.SH"
            ],
            "fields": "mfd_buyamt_d,mfd_sellamt_d",
            "options": "unit=1;traderType=1",
            "columns_map": {
                "MFD_BUYAMT_D" : "institution_buy_amount",
                "MFD_SELLAMT_D": "institution_sell_amount"
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d" 
        }
    },
    "CF_L": {
        "wsd_params": {
            "symbol": [
                "881001.WI", 
                "000300.SH", 
                "000905.SH", 
                "000906.SH", 
                "000852.SH"
            ],
            "fields": "mfd_buyamt_d,mfd_sellamt_d",
            "options": "unit=1;traderType=2",
            "columns_map": {
                "MFD_BUYAMT_D" : "large_buy_amount",
                "MFD_SELLAMT_D": "large_sell_amount"
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d" 
        }
    },
    "CF_M": {
        "wsd_params": {
            "symbol": [
                "881001.WI", 
                "000300.SH", 
                "000905.SH", 
                "000906.SH", 
                "000852.SH"
            ],
            "fields": "mfd_buyamt_d,mfd_sellamt_d",
            "options": "unit=1;traderType=3",
            "columns_map": {
                "MFD_BUYAMT_D" : "medium_buy_amount",
                "MFD_SELLAMT_D": "medium_sell_amount"
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d"
        }
    },
    "CF_R": {
        "wsd_params": {
            "symbol": [
                "881001.WI", 
                "000300.SH", 
                "000905.SH", 
                "000906.SH", 
                "000852.SH"
            ],
            "fields": "mfd_buyamt_d,mfd_sellamt_d",
            "options": "unit=1;traderType=4",
            "columns_map": {
                "MFD_BUYAMT_D" : "retail_buy_amount",
                "MFD_SELLAMT_D": "retail_sell_amount"
            }
        },
        "database": {
            "db_path": "dfs://wind_data",
            "table_name": "indicator_1d"
        }
    },
}

TABLE_RULES = {
    "dfs://market_data_1d_wind": {
        "index": None
    },
    "dfs://wind_data": {
        "indicator_1d": lambda df: df.set_index(["date", "code"]).stack().to_frame("data").reset_index(names=["date", "code", "field"])
    }
}

def _normalize_arg_list(arg, type_=str, header=None) -> List[Any]:
    """
    标准化参数为列表.

    Args:
        arg: 输入参数 (单值或列表)
        type_: 期望的类型
        header: 前置表头字段

    Returns:
        标准化后的列表
    """
    if arg is None:
        return None
    arg_list = [arg] if isinstance(arg, type_) else arg
    if header:
        return list(header) + list(arg_list)
    return arg_list

def broadcast(item: Union[Any, List[Any]], size: int) -> List[Any]:
    if not isinstance(item, list):
        return [item] * size
    
    if len(item) != size:
        raise ValueError(f"尺寸不匹配: 输入长度({len(item)}) != 目标尺寸({size})")

    return item

def transform_wind_data(w_data, remove_suffix=False, rename_cols=None):
    """
    转换w_data为DataFrame.
    
    Args:
        w_data (Wind EDB数据对象).
        remove_suffix (bool, optional): 是否移除代码后缀. Defaults to True.
        rename_cols (dict, optional): 列名重命名字典. Defaults to None.
        
    Returns:
        pd.DataFrame: 转换后的DataFrame.
    """
    data = pd.DataFrame(
        {col: val for col, val in zip(w_data.Fields, w_data.Data)},
        index = pd.to_datetime(w_data.Times)
    )
    symbol = w_data.Codes[0]
    if remove_suffix:
        symbol = symbol.split(".")[0]
    data.insert(0, "code", symbol)
    data.reset_index(names="date", inplace=True)
    data.sort_values(by="date", inplace=True)
    if rename_cols is not None:
        data.rename(columns=rename_cols, inplace=True)

    return data

def download_data_from_wind_wsd(
    symbol: Union[str, List[str]],
    start_date: Union[str, List[str]], 
    end_date: str,
    fields: str,
    options: str,
    columns_map: Dict[str, str]
) -> Tuple[pd.DataFrame, List[str]]:
    """
    市场数据WindWSD数据下载接口, 有容错机制, 单次请求失败不影响整体流程.

    Args:
        symbol (str, List[str]): 股票、指数、基金等代码.
        start_date (str, List[str]): 开始日期, 格式为"YYYY-MM-DD".
        end_date (str): 结束日期, 格式为"YYYY-MM-DD".
        fields (str): wsd fields 参数.
        options (str): wsd options 参数.
        columns_map (dict): 列名重命名字典.
    Returns:
        Tuple[pd.DataFrame, List[str]]: 包含数据的DataFrame和失败的代码列表.
    """
    symbols = _normalize_arg_list(symbol)
    start_date = broadcast(start_date, len(symbols))

    failed = []
    successful = []

    for code, begin_date in tqdm(zip(symbols, start_date), total=len(symbols)):
        try:
            w_data = w.wsd(code, fields, begin_date, end_date, options)
            # 检查是否有数据更新
            if not np.any(pd.to_datetime(w_data.Times) >= begin_date):
                continue

            error_code = w_data.ErrorCode
            if w_data.ErrorCode != 0:
                failed.append([code, error_code])
                continue
            
            data = transform_wind_data(w_data, rename_cols=columns_map)
            
            # Wind抽风的情况, 非正式账户会返回None值
            if data.empty:
                failed.append([code, "NONE!"])
                continue
            
            successful.append(data)

        except Exception as e:
            failed.append([code, 0])
            warnings.warn(f"{code}下载失败, 错误信息: {e}")

    print(f"下载完成, 任务统计: 成功{len(successful)} / 失败{len(failed)}")

    successful = pd.concat(successful) if successful else pd.DataFrame()
    failed = pd.DataFrame(failed, columns=["symbol", "error_code"]) if failed else pd.DataFrame()

    return successful, failed

def parse_date(value: str) -> str:
    normalized = value.replace("/", "-").replace(".", "-")
    if len(normalized) == 8 and normalized.isdigit():
        return pd.Timestamp(normalized).strftime("%Y-%m-%d")
    return pd.Timestamp(normalized).strftime("%Y-%m-%d")


def db_path_to_name(db_path: str) -> str:
    return db_path.replace("dfs://", "")


def save_parquet_by_date(data: pd.DataFrame, output_dir: str, db_path: str, table_name: str, rule_key: str) -> int:
    if data.empty:
        return 0

    saved = 0
    local_data = data.copy()
    local_data["date"] = pd.to_datetime(local_data["date"])

    for date_value, part in local_data.groupby(local_data["date"].dt.strftime("%Y%m%d")):
        year = date_value[:4]
        output_path = (
            Path(output_dir)
            / db_path_to_name(db_path)
            / table_name
            / rule_key
            / year
            / f"{date_value}.parquet"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        part.to_parquet(output_path, index=False, compression="snappy")
        print(f"asset_allocation | parquet | saved {len(part)} rows -> {output_path}")
        saved += len(part)
    return saved


def upload_to_dolphindb(data: pd.DataFrame, db_path: str, table_name: str) -> None:
    import dolphindb as ddb

    upload_data = data.copy()
    upload_data["date"] = pd.to_datetime(upload_data["date"]).dt.strftime("%Y.%m.%d")
    session = ddb.session()
    try:
        session.connect("127.0.0.1", 8902, "your_username", "your_password")
        session.upload({"temp_df": upload_data})
        script = f"""
            t = loadTable("{db_path}", "{table_name}")
            tableInsert(t, temp_df)
        """
        session.run(script)
        print(f"asset_allocation | upload | successfully uploaded {len(upload_data)} rows to {db_path}.{table_name}")
    finally:
        session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wind WSD asset-allocation market data updater.")
    parser.add_argument("--trade-date", type=parse_date, help="Single trade date, YYYYMMDD or YYYY-MM-DD.")
    parser.add_argument("--start-date", type=parse_date, help="Start date, YYYYMMDD or YYYY-MM-DD.")
    parser.add_argument("--end-date", type=parse_date, help="End date, YYYYMMDD or YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=3, help="Default lookback window when date args are omitted.")
    parser.add_argument("--output-dir", default=os.environ.get("OPS_ASSET_ALLOCATION_ROOT", "C:/quant_data/DolphinDB"))
    parser.add_argument("--dry-run", action="store_true", help="Skip DolphinDB writes (default).")
    parser.add_argument("--enable-write", action="store_true", help="Explicitly upload to DolphinDB.")
    parser.add_argument("--test-write", action="store_true", help="Write only to index_test/indicator_1d_test.")
    return parser.parse_args()


def resolve_date_range(args: argparse.Namespace) -> tuple[str, str]:
    if args.trade_date and (args.start_date or args.end_date):
        raise SystemExit("--trade-date cannot be combined with --start-date/--end-date")
    if args.trade_date:
        return args.trade_date, args.trade_date
    if args.start_date or args.end_date:
        end_date = args.end_date or pd.Timestamp.today().strftime("%Y-%m-%d")
        start_date = args.start_date or end_date
        if pd.Timestamp(start_date) > pd.Timestamp(end_date):
            raise SystemExit("--start-date must not be later than --end-date")
        return start_date, end_date
    end_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    start_date = (pd.Timestamp.today() - pd.Timedelta(days=args.lookback_days)).strftime("%Y-%m-%d")
    return start_date, end_date


def main() -> int:
    args = parse_args()
    start_date, end_date = resolve_date_range(args)
    enable_write = bool(args.enable_write)
    print(
        "asset_allocation | mode | "
        + ("production write enabled" if enable_write else "development dry-run, database writes disabled")
    )
    print(f"asset_allocation | plan | start_date={start_date}, end_date={end_date}, output_dir={args.output_dir}")

    start_wind()

    failed = []
    total_saved = 0
    pending_uploads = {}
    for key, val in DATA_RULES.items():
        print(f"asset_allocation | rule | {key} downloading...")
        data, failed_ = download_data_from_wind_wsd(start_date=start_date, end_date=end_date, **val["wsd_params"])
        if not failed_.empty:
            failed_.insert(0, "rule", key)
            failed.append(failed_)
        if data.empty:
            print(f"asset_allocation | rule | {key} no valid data")
            continue

        database_params = val["database"]
        db_path = database_params["db_path"]
        table_name = database_params["table_name"]
        transform = TABLE_RULES[db_path][table_name]
        if transform:
            data = transform(data)

        total_saved += save_parquet_by_date(data, args.output_dir, db_path, table_name, key)

        pending_uploads.setdefault((db_path, table_name), []).append(data)
        if not enable_write:
            print(f"asset_allocation | upload | dry-run, DolphinDB write skipped for {key}, rows={len(data)}")

    if enable_write:
        for (db_path, table_name), frames in pending_uploads.items():
            combined = pd.concat(frames, ignore_index=True)
            if args.test_write:
                key_columns = ("code", "date", "field") if "field" in combined.columns else ("code", "date")
                for date_value, date_frame in combined.groupby(pd.to_datetime(combined["date"]).dt.strftime("%Y%m%d")):
                    write_test_table_by_date(
                        date_frame, db_path, table_name, date_value, key_columns=key_columns
                    )
            else:
                upload_to_dolphindb(combined, db_path, table_name)

    if failed:
        failed_df = pd.concat(failed, ignore_index=True)
        failed_path = Path(args.output_dir) / "asset_allocation_failed" / f"{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        failed_df.to_parquet(failed_path, index=False, compression="snappy")
        print(f"asset_allocation | failed | saved failed list -> {failed_path}")

    print(f"asset_allocation | done | total_saved_rows={total_saved}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
