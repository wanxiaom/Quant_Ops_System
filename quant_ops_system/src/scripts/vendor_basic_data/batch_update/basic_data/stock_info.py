""" 批量更新: 股票信息库 """

__version__ = 0
__author__ = "your_username"


# 导入模块
import rqdatac
from lib import utils, connection
from tables.basic_data import stock_info


# 主程序
def main():
    print(f'---------- stock_info ----------')
    timer = utils.Timer()
    with connection.Connection() as conn:
        for module in (stock_info.shares,
                       stock_info.ex_factor,

                       stock_info.market_value,
                       stock_info.turnover,
                       stock_info.ex_quote,
                       stock_info.tradable):
            table = module.Table()
            table.conn = conn
            table.create_table()
            table.update_table()
    timer.finish()


# 执行测试
if __name__ == '__main__':
    rqdatac.init()  # type: ignore
    main()
