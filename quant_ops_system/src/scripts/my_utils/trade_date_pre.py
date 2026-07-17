import os
import dolphindb as ddb
import pandas as pd
from datetime import datetime, timedelta


def get_trade_dates_end_of_yesterday(
    start_date: str = None,
    end_date: str = None
) -> list:
    """
    获取 DolphinDB 表中指定日期范围内的所有交易日（格式：'YYYYMMDD'）

    Args:
        start_date (str, optional): 起始日期，格式 'YYYYMMDD'，默认为 '20000101'
        end_date (str, optional):   结束日期，格式 'YYYYMMDD'，默认为昨天

    Returns:
        list of str: e.g., ['20200102', '20200103', ..., '20251209'] （升序）
    """
    # 设置默认值
    if start_date is None:
        start_date = "20000101"
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    s = ddb.session()
    try:
        s.connect("127.0.0.1", 8902, "your_username", "your_password")

        script = f"""
            t = loadTable("dfs://basic_info", "trade_date_pre")
            select distinct date from t 
            where date between temporalParse("{start_date}", "yyyyMMdd") and temporalParse("{end_date}", "yyyyMMdd") 
            order by date
        """
        dates_df = s.run(script)

        if dates_df.empty:
            return []

        # 转为 %Y%m%d 字符串列表（确保是字符串，不是 Timestamp）
        dates_list = pd.to_datetime(dates_df['date']).dt.strftime('%Y%m%d').tolist()
        return dates_list

    finally:
        s.close()


def get_trade_dates_end_of_today(
    start_date: str = None,
    end_date: str = None
) -> list:
    """
    获取 DolphinDB 表中指定日期范围内的所有交易日（格式：'YYYYMMDD'）

    Args:
        start_date (str, optional): 起始日期，格式 'YYYYMMDD'，默认为 '20000101'
        end_date (str, optional):   结束日期，格式 'YYYYMMDD'，默认为今天

    Returns:
        list of str: e.g., ['20200102', '20200103', ..., '20251209'] （升序）
    """
    # 设置默认值
    if start_date is None:
        start_date = "20000101"
    if end_date is None:
        end_date = datetime.now().strftime('%Y%m%d')

    s = ddb.session()
    try:
        s.connect("127.0.0.1", 8902, "your_username", "your_password")

        script = f"""
            t = loadTable("dfs://basic_info", "trade_date_pre")
            select distinct date from t 
            where date between temporalParse("{start_date}", "yyyyMMdd") and temporalParse("{end_date}", "yyyyMMdd") 
            order by date
        """
        dates_df = s.run(script)

        if dates_df.empty:
            return []

        # 转为 %Y%m%d 字符串列表（确保是字符串，不是 Timestamp）
        dates_list = pd.to_datetime(dates_df['date']).dt.strftime('%Y%m%d').tolist()
        return dates_list

    finally:
        s.close()


def is_trade_date(date: str = None) -> bool:
    """
    判断给定日期是否为交易日（存在于 trade_date_pre 表中）

    Args:
        date: 日期字符串，格式 'YYYYMMDD'，默认为今天

    Returns:
        bool: True / False
    """
    if date is None:
        date = datetime.now().strftime('%Y%m%d')

    s = ddb.session()
    try:
        s.connect("-", -, "your_username", "your_password")

        script = f"""
            t = loadTable("dfs://basic_info", "trade_date_pre")
            exists = exec count(*) from t where date = temporalParse("{date}", "yyyyMMdd")
            exists > 0
        """
        result = s.run(script)
        return bool(result)

    finally:
        s.close()
