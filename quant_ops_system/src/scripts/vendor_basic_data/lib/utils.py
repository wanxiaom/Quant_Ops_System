""" 功能函数 """

__version__ = 0
__author__ = 'Lin Shan'


import datetime
import typing


class Timer(object):
    """计时器"""

    def __init__(self):
        self.start_time = datetime.datetime.now()  # 起始时间
        print(f"□ Program started at {self.start_time.strftime('%H:%M:%S')}")

    def finish(self) -> None:
        """完成计时"""
        finish_time = datetime.datetime.now()  # 终止时间
        used_time = str(finish_time - self.start_time).split('.')[0]  # 总耗时
        print(f"□ Program finished at {finish_time.strftime('%H:%M:%S')}, time used overall: {used_time}")


class Counter(object):
    """计数器"""

    def __init__(self, ttl: int, table_name: str, unit: str = 'date'):
        self.ttl = ttl  # 总数字
        self.cnt = 0  # 计数字
        self.table_name = table_name  # 数据表名
        self.unit = unit  # 单位
        print(f'\r■ Table {self.table_name} updating: 0/{self.ttl} {unit} processed...', end='', flush=True)

    def count(self) -> None:
        """计数"""
        self.cnt += 1
        unit = f"{self.unit}{'' if (self.cnt <= 1) else 's'}"
        print(f'\r■ Table {self.table_name} updating: {self.cnt}/{self.ttl} {unit} processed...', end='')

    def finish(self) -> None:
        """完成计数"""
        unit = f"{self.unit}{'' if (self.ttl <= 1) else 's'}"
        print(f'\r□ Table {self.table_name} updated for {self.ttl} {unit}             ', flush=True)


def if_null(check_value: typing.Any, replacement_value: typing.Any) -> typing.Any:
    """空值替换"""
    return check_value if check_value else replacement_value

def if_empty(check_value: typing.Any, replacement_value: typing.Any) -> typing.Any:
    """空表替换"""
    return check_value if not check_value.empty else replacement_value

def ddb_date(date: typing.Any) -> str:
    """DolphinDB日期格式转换"""
    return str(date).replace('-', '.')
