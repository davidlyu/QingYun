#encoding=utf-8

"""
Strategy hierarchy.
A Strategy object encapsulates all calculations on market data that generate advisory signals
to a Portfolio object. 

author: lvbj
date: 201-1-5
"""

# strategy.py

import datetime
import numpy as np
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import SignalEvent


# strategy.py

class Strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    The goal of a (derived) Strategy object is to generate Signal
    objects for particular symbols based on the inputs of Bars 
    (OLHCVI) generated by a DataHandler object.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source,
    since it obtains the bar tuples from a queue object.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")


class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the 
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    START_TEST_DATE = "2017-08-07"
    END_TEST_DATE = "2018-12-31"

    def __init__(self, bars, backtester):
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        backtester - The Backtester object.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.backtester = backtester

        # Once buy & hold signal is given, these are set to True
        self.bought = self._calculate_initial_bought()


    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought


    def calculate_signals(self, event):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are 
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object. 
        """
        if event.kind == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)
                if bars is not None and bars != []:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal = SignalEvent(bars[0].symbol, bars[0].dt, 'LONG')
                        self.backtester.send_event(signal)
                        self.bought[s] = True