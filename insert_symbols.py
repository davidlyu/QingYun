# encoding=utf-8

# insert_symbols.py

import datetime
import requests
from math import ceil
import pandas as pd

def obtain_hs300():
    """
    Download 中证指数有限公司沪深300成份股列表,
    Returns a pandas.DataFrame for to store to csv file.
    """
    # Stores the current time, for the created_at record
    now = datetime.datetime.utcnow()

    url = "http://www.csindex.com.cn/uploads/file/autofile/cons/000300cons.xls"
    res = requests.get(url)

    # Store hs300 list to excel file
    excel_file = open("hs300.xls", "wb")
    excel_file.write(res.content)
    excel_file.close()

    symbols = pd.read_excel("hs300.xls", usecols=[0, 4, 5, 7], names=["last_updated_date", "ticker", "name", "exchange_id"])
    symbols["instrument"] = "stock"
    symbols["currency"] = "RMB"
    symbols["created"] = now.strftime("%Y-%m=%d")

    # change ticker datetype to str and pad with "0" to length of 6
    symbols["ticker"] = symbols.ticker.astype("str", copy=True)
    symbols["ticker"] = symbols.ticker.str.pad(6, side="left", fillchar="0")

    return symbols

def store_hs300_symbols(symbols, path=None):
    """
    Store the hs300 symbols into csv file
    path - the path where the csv file write to.
    """
    if path is None:
        path = "datas/securities_master/symbol.csv"
    symbols.to_csv(path, encoding="utf-8")


if __name__ == "__main__":
    symbols = obtain_hs300()
    store_hs300_symbols(symbols)
    print("{} symbols were successfully added.".format(len(symbols)))

