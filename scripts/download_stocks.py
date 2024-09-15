#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/PySwing
#
import pandas as pd
import yfinance as yf
import warnings
import os
import gdown

warnings.filterwarnings("ignore")
from _utils import DataHandler

def download_past_results(file_path):
    """
    This functions downloads the last few years backtesting data. The file size is around 1.3 GB
    """
    if os.path.exists(file_path):
        pass
    else:
        url = "https://drive.google.com/uc?export=download&id=1qhlBxYU3EEI21qX3oppn2sb0F3JNkHtv"       
        gdown.download(url, file_path, quiet=False)

def download_main(nse_list_final, start_date="2023-01-01"):
    """
    This function uses python multiprocessing to download all stocks in the input list.
    """
    main_stock_df = pd.DataFrame()
    try:
        for ticker in nse_list_final:
            next_df = yf.download(ticker, interval="1d", start=start_date)
            inter_df = pd.DataFrame()
            next_df = next_df.reset_index()
            next_df = next_df.rename(columns={"Datetime": "date"})
            next_df["stock"] = ticker
            main_stock_df = pd.concat([main_stock_df, next_df])
    except Exception as e:
        DataHandler.log_error(e)

    return main_stock_df


def process_symbols(symbols):
    """
    This function calls the _download_main function
    """
    main_stock_df = download_main(symbols)
    return main_stock_df
