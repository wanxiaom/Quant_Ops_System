import os
from WindPy import w
import time
import dolphindb as ddb
import pandas as pd
from datetime import datetime

w.start()
while not w.isconnected():
    print("Waiting for Wind connection...")
    time.sleep(1)


def get_wind_code(
    trade_date: str = None
) -> list:
    """
    获取 DolphinDB 表中指定日期范围内的所有交易日（格式：'YYYYMMDD'）

    Args:
        trade_date (str, optional):   交易日期，格式 'YYYYMMDD'，默认为今天
    """
    # 设置默认值
    if trade_date is None:
        trade_date = datetime.now().strftime('%Y%m%d')

    s = ddb.session()
    try:
        s.connect("", , "your_username", "your_password")

        script = f"""
            t = loadTable("dfs://basic_info", "wind_code")
            select code from t where date = temporalParse("{trade_date}", "yyyyMMdd") 
        """
        df = s.run(script)

        if df.empty:
            return fetch_wind_code(trade_date)

        # 转为 %Y%m%d 字符串列表（确保是字符串，不是 Timestamp）
        codes = df['code'].tolist()
        return codes

    finally:
        s.close()


def fetch_wind_code(trade_date):
    print(f" fetch wind code for date: {trade_date}")
    res = w.wset("sectorconstituent", date=trade_date, sectorid="a001010100000000", field="wind_code,sec_name")

    codes = res.Data[0]
    if codes is None or len(codes) <= 1:
        print("  res: ", res)
        print("  codes: ", codes)
        print("  fetch wind code failed")
        return

    df = pd.DataFrame({
        'date': pd.to_datetime(trade_date, format='%Y%m%d'),
        'code': [c for c in codes]
    })

    s = ddb.session()
    s.connect("127.0.0.1", 8902, "your_username", "your_password")

    s.upload({'tmp_tb': df})
    s.run("""
        db = database('dfs://basic_info')
        pt = db.loadTable('wind_code')
        pt.append!(tmp_tb)
    """)
    s.close()

    print(f"  uploaded wind code successfully: {len(df)} codes")
    return codes
