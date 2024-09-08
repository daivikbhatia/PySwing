#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/swing_trading
#
import pandas as pd
import multiprocessing
from download_stocks import process_symbols, download_past_results
from _utils import DataHandler

if __name__ == "__main__":
    try:
        download_past_results("../data/input/main_buyit_jan_2024.csv")
        nse_list_final = DataHandler.symbol_maker("../data/input/ind_nifty500list.csv")
        num_processes = multiprocessing.cpu_count()
        chunk_size = len(nse_list_final) // num_processes
        symbol_chunks = [
            nse_list_final[i : i + chunk_size]
            for i in range(0, len(nse_list_final), chunk_size)
        ]
        with multiprocessing.Pool(processes=num_processes) as pool:
            results = pool.map(process_symbols, symbol_chunks)

        main_stock_dfs = results
        main_stock_df = pd.concat(main_stock_dfs, ignore_index=True)
        main_stock_df.to_csv("../data/processed/main_stock_df.csv")
    except Exception as e:
        DataHandler.log_error(e)
