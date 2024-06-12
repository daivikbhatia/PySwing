import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
import talib
import warnings
import niftystocks
import time

warnings.filterwarnings("ignore")
import os
import mplfinance as mpf
import matplotlib.pyplot as plt
import ImageMagic
import imageio
import sys
from fpdf import FPDF
import shutil


def trading_engine(nse_list_final, start_date="2023-1-05"):
    main_stock_df = pd.DataFrame()
    try:
        for ticker in nse_list_final:
            next_df = yf.download(ticker, interval="1d", start="2023-01-01")
            inter_df = pd.DataFrame()
            next_df = next_df.reset_index()
            next_df = next_df.rename(columns={"Datetime": "date"})
            next_df["stock"] = ticker
            main_stock_df = pd.concat([main_stock_df, next_df])
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno
        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
        pass

    return main_stock_df


def process_symbols(symbols):
    main_stock_df = trading_engine(symbols)
    return main_stock_df
