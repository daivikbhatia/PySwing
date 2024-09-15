#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/PySwing
#
import pandas as pd
from itertools import product
import multiprocessing
from backtest_intraday import TradingEngine, BackTestEngine
from _utils import DataHandler, Strategy
import time


def main_crossover_fn(the_strats):
    """
    This function runs the entire SMA crossover strategy for given SMAs and stocks data.
    """
    count = 0
    nse = pd.read_csv("../data/input/ind_nifty500list.csv")
    nse_list = list(nse["Symbol"])
    start_date = "2023-01-10"
    nse_list_final = []
    for i in nse_list:
        i = str(i)
        j = i + ".NS"
        nse_list_final.append(j)
    mains_df = pd.read_csv("../data/processed/main_stock_df.csv", index_col=0)

    for hpt in the_strats:
        try:
            count = count + 1
            print(count)
            start = time.time()
            before_co = hpt[5]
            sma = hpt[0]
            ema = hpt[1]
            sma_len = hpt[2]
            ema_len = hpt[3]
            atr_rot = hpt[4]
            trading_engine = TradingEngine(
                sma,
                ema,
                atr_rot,
                sma_len,
                ema_len,
                before_co,
                nse_list_final,
                mains_df=mains_df,
            )
            all_df = trading_engine.main_engine()
            backTest_engine = BackTestEngine(
                start_date,
                sma,
                ema,
                atr_rot,
                sma_len,
                ema_len,
                before_co,
                mains_df,
                all_df,
            )

            backTest_engine.run_bactTest()

            y_df = backTest_engine.run_bactTest()
            stop = time.time()
            print("total_time: ", stop - start)
        except Exception as e:
            print(e)
            pass


# cross_join_data = list(product(emas, smas, ema_tail, sma_tail))
if __name__ == "__main__":
    num_processes = multiprocessing.cpu_count()
    great_df = pd.read_csv("../data/input/final_stratergy_jan_2024.csv", index_col=0)
    great_df = great_df[["new_hpt_lis"]].drop_duplicates()
    today_date = DataHandler.date_20_days_from_now()

    great_df["true_hpts"] = great_df["new_hpt_lis"].apply(eval)
    best_hpt_list = list(great_df["true_hpts"])
    DataHandler.reset_dataFrames()

    chunk_size = len(best_hpt_list) // 7
    # symbol_chunks = [cross_join_data[i:i+chunk_size] for i in range(0, len(cross_join_data), chunk_size)]
    symbol_chunks = [
        best_hpt_list[i : i + chunk_size]
        for i in range(0, len(best_hpt_list), chunk_size)
    ]

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(main_crossover_fn, symbol_chunks)
