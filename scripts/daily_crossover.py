from itertools import product
import multiprocessing
from ma_test import *
from backtest_intraday import *
from datetime import datetime, timedelta


def date_20_days_from_now():
    today = datetime.today()
    past_date = today - timedelta(days=20)
    return past_date.strftime("%Y-%m-%d")


num_processes = multiprocessing.cpu_count()
today_date = date_20_days_from_now()

great_df = pd.read_csv("./final_stratergy_jan_2024.csv", index_col=0)
great_df = great_df[["new_hpt_lis"]].drop_duplicates()

great_df["true_hpts"] = great_df["new_hpt_lis"].apply(eval)
best_hpt_list = list(great_df["true_hpts"])

# smas = [i for i in range(150,200,10)]
# emas = [i for i in range(20,70,10)]
# sma_tail = [i for i in range(5, 45, 5)]
# ema_tail = [i for i in range(5, 45, 5)]
# atr_rot = [1,2,3,4]
# intersect_point = [True]
# best_hpt_list = list(product(emas, smas, ema_tail, sma_tail,atr_rot,intersect_point))


def main_crossover_fn(the_strats):
    count = 0
    nse = pd.read_csv("./ind_nifty500list.csv")
    nse_list = list(nse["Symbol"])
    start_date = "2023-01-10"
    nse_list_final = []
    for i in nse_list:
        i = str(i)
        j = i + ".NS"
        nse_list_final.append(j)
    mains_df = pd.read_csv("./main_stock_df.csv", index_col=0)

    for hpt in the_strats:
        try:
            count = count + 1
            print(count)
            start = time.time()
            before_co = hpt[5]
            all_df, ma_val = trading_engine(before_co, hpt, mains_df, nse_list_final)
            sma = hpt[0]
            ema = hpt[1]
            sma_len = hpt[2]
            ema_len = hpt[3]
            atr = hpt[4]
            y_df = backtest_engine_fn(
                pd, all_df, sma, ema, sma_len, ema_len, atr, start=today_date
            )
            # y_df = backtest_engine_fn(pd,all_df,sma,ema, sma_len, ema_len,atr
            #                          ,start="2021-01-01",end="2022-01-01")
            # y_df = backtest_engine_fn(pd,all_df,sma,ema, sma_len, ema_len,atr
            #                          ,start="2022-01-01",end="2023-01-01")
            # y_df = backtest_engine_fn(pd,all_df,sma,ema, sma_len, ema_len,atr
            #                          ,start="2023-01-01",end="2023-11-01")
            # y_df = backtest_engine_fn(pd,all_df,sma,ema, sma_len, ema_len,atr
            #                          ,start="2023-08-01",end="2023-11-01")
            stop = time.time()
            print("total_time: ", stop - start)
        except Exception as e:
            print(e)
            pass


# cross_join_data = list(product(emas, smas, ema_tail, sma_tail))
if __name__ == "__main__":
    chunk_size = len(best_hpt_list) // 7
    # symbol_chunks = [cross_join_data[i:i+chunk_size] for i in range(0, len(cross_join_data), chunk_size)]
    symbol_chunks = [
        best_hpt_list[i : i + chunk_size]
        for i in range(0, len(best_hpt_list), chunk_size)
    ]

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(main_crossover_fn, symbol_chunks)
