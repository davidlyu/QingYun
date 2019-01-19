#encoding=utf-8

"""
The data hierarchy. The DataHandler is an abstract base class providing an interface.
The HistoricCSVDataHandler class is a example, need to be modified.

author: lvbj
date: 201-1-5
"""

from datetime import datetime
import os, os.path
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent
from bar import Bar

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, backtester, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        backtester - The Backtester.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.backtester = backtester
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True       

        self._open_convert_csv_files()


    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from DTN IQFeed. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                                      os.path.join(self.csv_dir, '%s.csv' % s),
                                      header=0, index_col=0, 
                                      names=['datetime','open','low','high','close','volume','oi'],
                                  )

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()


    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of 
        (sybmbol, datetime, open, low, high, close, volume).
        """
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'), 
                        b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])


    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]


    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.backtester.send_event(MarketEvent())


class CoinDataHandler(DataHandler):
    """
    CoinDataHandler 读取数字货币的tick数据的csv文件，提供一个获取最新的bar数据的接口，
    与实盘交易相同的方式。
    """
    def __init__(self, backtester, symbol_list, benchmark_symbol="okcoinUSD"):
        """
        Parameter:
        backtester - BackTester object
        symbol_list - list of digital coin symbols, the symbol equal to data
                    filename without '.csv'.
        benchmark_symbol - symbol of benchmark.
        """
        self.backtester = backtester
        self.symbol_list = symbol_list
        self.benchmark_symbol = benchmark_symbol

        self.symbol_data = {}

        self.latest_symbol_data = {}
        for k in symbol_list:
            self.latest_symbol_data[k] = []

        self.continue_backtest = True
        self.current_datetime = None
        self.__benchmarks = []

        self._open_convert_csv_files()

    @staticmethod
    def _tick2bar(df):
        """
        将pd.DataFrame类型的tick文件，转换成bar数据类型的DataFrame类型。
        即，把(timestamp, price, volume)类型的数据转换成
        (datetime, open, high, low, close, volume)的数据。
        """
    
        df['dt'] = df['timestamp'].map(datetime.fromtimestamp)
        
        p_list = []
        v_list = []
        bar_df = pd.DataFrame()

        dt = df.loc[0, 'dt']
        for i in range(len(df)):
            tick = df.iloc[i, :]
            if dt.minute != tick['dt'].minute:
                if len(p_list) != 0 and len(v_list) != 0:
                    open_ = p_list[0]
                    hi = max(p_list)
                    lo = min(p_list)
                    clos = p_list[-1]
                    vol = sum(v_list)

                    p_list.clear()
                    v_list.clear()
                    bar_df = bar_df.append(
                        {'datetime': dt,
                         'open': open_,
                         'high': hi,
                         'low': lo,
                         'close': clos,
                         'volume': vol},
                         ignore_index=True
                    )
                    dt = tick['dt']
            p_list.append(tick.price)
            v_list.append(tick.volume)

        converter = lambda dt: dt.strftime("%Y-%m-%d %H:%M:00")
        bar_df['datetime'] = bar_df['datetime'].map(converter)
        return bar_df
    
    def _open_convert_csv_files(self):
        """
        打开tick数据的csv文件，并将其转换成bar数据类型。
        i.e. 把(timestamp, price, volume)类型的数据转换成
        (datetime, open, high, low, close, volume)的数据。
        """
        comb_index = None
        for s in self.symbol_list:
            filename = "datas/{}.csv".format(s)
            coin_df = pd.read_csv(filename, names=['timestamp', 'price', 'volume'],
                        header=0, nrows=100000)
            bar_df = self._tick2bar(coin_df)
            self.symbol_data[s] = bar_df

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()


    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed as a Bar object.
        (sybmbol, datetime, open, low, high, close, volume).
        """
        for b in self.symbol_data[symbol]:
            args = {'symbol': symbol, 'dt': b[1]['datetime'], 'open': b[1]['open'],
                    'high': b[1]['high'], 'low': b[1]['low'], 'close': b[1]['close'],
                    'volume': b[1]['volume']}
            yield(Bar(**args))
                

    def get_latest_bars(self, symbol, N=1):
        """ 
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]


    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure for
        all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        e = MarketEvent()
        self.backtester.send_event(e)
