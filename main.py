#encoding=utf-8

"""
Qingyun 量化回测系统的入口
author: lvbj
date: 201-1-5
"""

from queue import Queue, Empty
from threading import Thread
import time

from data import CoinDataHandler
from strategy import  BuyAndHoldStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler


class Backtester:
    def __init__(self, bars=None, strategy=None, port=None, broker=None):

        if bars is None:
            bars = CoinDataHandler(self, ['okcoinUSD'])
            
        if strategy is None:
            strategy = BuyAndHoldStrategy(bars, self)

        if port is None:
            port = NaivePortfolio(bars, self, '2017-1-1')
 
        if broker is None:
            broker = SimulatedExecutionHandler(self)

        self.bars = bars
        self.strategy = strategy
        self.port = port
        self.broker = broker

        self.__event_queue = Queue()
        self.__thread = Thread(target=self.__run)
        self.__active = False
        self.__handlers = {
            'MARKET': [strategy.calculate_signals, port.update_timeindex],
            'SIGNAL': [port.update_signal],
            'ORDER': [broker.execute_order],
            'FILL': [port.update_fill]}

        self.__start_date = None
        self.__end_date = None


    def __filte_market_event(self, event):
        """
        过滤MarketEvent，如果MarketEvent的日期在start_date至end_date之间.
        将MarketEvent传递给strategy.calculate_signals和port.update_timeindex，
        否则，则什么也不做。
        """
        pass
        
        
    def __run(self):
        """
        Backtester运行
        """
        # 处理事件队列中的事件，直到事件队列为空
        while True:
            try:
                event = self.__event_queue.get(block=False)
                if event.kind in self.__handlers:
                    for handler in self.__handlers[event.kind]:
                        handler(event)
            except Empty:
                if self.bars.continue_backtest:
                    self.bars.update_bars()
                else:
                    break
        self.port.create_equity_curve_dataframe()
        stats = self.port.output_summary_stats()
        print(stats)

    def start(self):
        """
        回测开始
        """
        self.__active = True
        self.__thread.start()


    def stop(self):
        """
        回测结束
        """
        self.__active = False


    def send_event(self, event):
        """
        向Backtester加入需要进行处理的事件

        parameter:
        event - 需要进行处理的事件,Event类型
        """
        self.__event_queue.put(event)


if __name__ == '__main__':
    import datetime
    time1 = datetime.datetime.now()
    tester = Backtester()

    tester.start()
    total_seconds = (datetime.datetime.now() - time1).seconds
    print("Backtest is running with a total of {} seconds.".format(total_seconds))