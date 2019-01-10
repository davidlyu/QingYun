#encoding=utf-8

"""
Bar数据类型，在一段时间内的开盘价，收盘价，最高，最低价，成交量等信息。

author: lvbj
date: 2019-1-9
"""

import datetime

class Bar(object):
    """
    Bar数据类型，在一段时间内的开盘价，收盘价，最高，最低价，成交量等信息。
    """
    def __init__(self, symbol, dt, open, high, low, close, volume):
        """
        Parameter:
        symbol - 标的代码
        dt - 日期时间, 可以形如'%Y-%m-%d %H:%M:%S'的字符串，
            可以是datetime.datetime类型的数据。
        open - 开盘价
        high - 最高价
        low - 最低价
        close - 收盘价
        volume - 成交量
        """
        self.symbol = symbol

        if isinstance(dt, str):
            try:
                self.dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError("{} and '%Y-%m-%d %H:%M:%S' can’t be parsed by time.strptime()".format(dt))

            self.strtime = dt

        if isinstance(dt, datetime.datetime):
            self.dt = dt
            self.strtime = self.dt.strftime("%Y-%m-%d %H:%M:%S")

        if high < max(open, high, low, close):
            raise ValueError("Error: high should be the maximum of open, high, low, close")

        if low > min(open, high, low, close):
            raise ValueError("Error: low should be the minimum of open, high, low, close")

        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __repr__(self):
        items = ['symbol', 'strtime', 'open', 'high', 'low', 'close', 'volume']
        s = ""
        for item in items:
            s += "{key}:{value}  ".format(key=item, value=self.__dict__[item])
        return s


if __name__ == '__main__':
    arg = {"symbol": "coin",
            "dt": "2012-12-21 12:12:12",
            "open": 12.3,
            "high": 12.0,
            "low": 12.1,
            "close": 12,
            "volume": 1}
    bar = Bar(**arg)
    print(bar)
