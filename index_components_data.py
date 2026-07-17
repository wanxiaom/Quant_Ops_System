#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import datetime
import pandas as pd
import rqdatac as rq
import dolphindb as ddb

# 1. 配置标准日志 (Agent 将捕获此输出)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

try:
    # 提前初始化米筐接口，以便后续调用交易日历
    rq.init(os.getenv("RQ_USERNAME", "your_username"), os.getenv("RQ_PASSWORD", "your_password"), os.getenv("RQ_LICENSE_KEY", "your_rq_license_key"))
except Exception as e:
    logging.error(f"Failed to initialize rqdatac. Error: {e}")
    sys.exit(1)

def fetch_index_components(trade_date=None):
    # 定义需要抓取成分股的核心宽基指数，包含指定的 19 个基准指数，并硬编码公司内部的指数代码
    indices = {
        
    }

    if trade_date is None:
        trade_date = datetime.date.today().strftime('%Y-%m-%d')
    all_data = []

    for order_book_id, (name, target_code) in indices.items():
        logging.info(f"Fetching components and weights for {name} ({order_book_id}) on {trade_date}...")
        try:
            # 获取成分股及权重，返回值为 pandas.Series (索引为股票代码，值为权重)
            weights_series = rq.index_weights(order_book_id, date=trade_date)
            
            if weights_series is not None and not weights_series.empty:
                # 转化为 DataFrame 并格式化字段
                # 将米筐的后缀 .XSHG / .XSHE 转换为公司格式 .SH / .SZ
                df = pd.DataFrame({
                    'stock_code': weights_series.index.str.replace('.XSHG', '.SH', regex=False).str.replace('.XSHE', '.SZ', regex=False),
                    'weight': weights_series.values
                })
                
                # 直接使用硬编码的指定格式目标代码
                df['code'] = target_code
                    
                # 将日期格式从 YYYY-MM-DD 改为 YYYY.MM.DD
                df['date'] = trade_date.replace('-', '.')

                # 仅提取需要的四列指标
                df = df[['code', 'date', 'stock_code', 'weight']]
                
                all_data.append(df)
                logging.info(f"Successfully fetched {len(df)} components for {name}.")
                
            else:
                logging.warning(f"No data returned for {name} on {trade_date}.")
        except Exception as e:
            logging.error(f"Error fetching data for {name}: {e}")

    if not all_data:
        logging.warning(f"No data fetched on {trade_date} (possibly early years before index creation). Skipping.")
        return pd.DataFrame()

    # 合并所有指数的数据
    final_df = pd.concat(all_data, ignore_index=True)
    logging.info(f" Successfully fetched all index components ({len(final_df)} rows) into memory")
    return final_df

def upload_to_dolphindb(trade_date: str, df: pd.DataFrame):
    logging.info(f"Starting DolphinDB upload for {trade_date}...")
    if df is None or df.empty:
        logging.warning("No data to upload.")
        return

    try:
        s = ddb.session()
        s.connect("--", --, "your_username", "your_password")
        s.upload({'tmp': df})
        
        # 在 basic_data(catalog) 目录下的 index_info 数据库中新建并写入 index_component 表
        s.run("""
            tmp_parsed = select code, temporalParse(date, 'yyyy.MM.dd') as date, stock_code, weight from tmp;
            
            db_path = "dfs://index_info";
            
            // 1. 如果数据库不存在，则新建 (默认按日期 VALUE 分区)
            if(existsDatabase(db_path)){
                db = database(db_path);
            } else {
                db = database(db_path, VALUE, 2020.01.01..2035.12.31);
            }
            
            // 2. 如果表不存在，则新建分布式表
            if(existsTable(db_path, "index_component")){
                pt = loadTable(db, "index_component");
            } else {
                pt = db.createPartitionedTable(table=tmp_parsed, tableName="index_component", partitionColumns="date", sortColumns=`code`stock_code`date);
            }
            
            // 3. 追加数据
            pt.append!(tmp_parsed);
        """)
        s.close()
        logging.info(f"Successfully uploaded {len(df)} rows to DolphinDB.")
    except Exception as e:
        logging.error(f"DolphinDB upload failed: {e}")

if __name__ == "__main__":
    logging.info("--- Starting Ricequant Index Components Task ---")

    dates_to_run = []
    
    # 支持传入两个参数作为起始和结束日期：python index_components_data.py 2000-01-01 2026-06-09
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        
        if len(start_date) == 8 and start_date.isdigit():
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        if len(end_date) == 8 and end_date.isdigit():
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            
        logging.info(f"Fetching trading dates from {start_date} to {end_date}...")
        trading_dates = rq.get_trading_dates(start_date, end_date)
        dates_to_run = [d.strftime('%Y-%m-%d') for d in trading_dates]
        
    # 传入单个日期
    elif len(sys.argv) == 2:
        input_date = sys.argv[1]
        if len(input_date) == 8 and input_date.isdigit():
            target_date = f"{input_date[:4]}-{input_date[4:6]}-{input_date[6:]}"
        else:
            target_date = input_date
        dates_to_run = [target_date]
        
    # 默认抓取今天
    else:
        dates_to_run = [datetime.date.today().strftime('%Y-%m-%d')]

    # 循环遍历每个需要抓取的交易日
    for target_date in dates_to_run:
        df = fetch_index_components(target_date)
        if df is not None and not df.empty:
            upload_to_dolphindb(target_date, df)
        
    logging.info("--- Task Completed Successfully ---")
