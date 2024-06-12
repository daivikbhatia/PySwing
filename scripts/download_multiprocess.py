import pandas as pd
import multiprocessing

from download_stocks import *

nse = pd.read_csv("./ind_nifty500list.csv")
nse_list = list(nse["Symbol"])
start_date = "2023-09-05"
nse_list_final = []

for i in nse_list:
    i = str(i)
    j = i + ".NS"
    nse_list_final.append(j)

num_processes = multiprocessing.cpu_count()  # Use the number of available CPU cores


def add_hpt(temp):
    temp["hpt_list"] = "empty"
    temp.reset_index(drop=True, inplace=True)
    for index, row in temp.iterrows():
        hpt = []
        hpt.append(row["sma"])
        hpt.append(row["ema"])
        hpt.append(row["sma_len"])
        hpt.append(row["ema_len"])
        hpt.append(row["atr_rot"])
        temp.loc[index, "hpt_list"] = str(hpt)
    return temp


buyit_df = pd.read_csv("./main_buyit.csv", index_col=0)
buyit_df["date"] = pd.to_datetime(buyit_df["date"])
buyit_df = buyit_df.sort_values(["date"]).reset_index(drop=True)
buyit_df.reset_index(drop=True, inplace=True)
buyit_df.shape

new_res_df = pd.read_csv("./paisabnaya_results.csv", index_col=0)
new_res_df["expected"] = (new_res_df["profit_count"] * 2000) - (
    new_res_df["loss_count"] * 1000
)
new_res_df = add_hpt(new_res_df)

res_df = pd.read_csv("./dhanteras_all_res_data.csv", index_col=0)
galla_df = pd.read_csv("./dhanteras_all_galla_data.csv", index_col=0)

res_df["expected"] = (res_df["profit_count"] * 2000) - (res_df["loss_count"] * 1000)

columns = res_df.columns
columns = list(columns)
new_df = pd.DataFrame(columns=columns)
new_df.to_csv("./paisabnaya_results.csv")
tempdf = pd.DataFrame(columns=galla_df.columns)
tempdf.to_csv("./backtest_galla.csv")
pd.DataFrame(columns=buyit_df.columns).to_csv("./main_buyit.csv")

if __name__ == "__main__":
    # Split the list of symbols into chunks to distribute to processes
    chunk_size = len(nse_list_final) // num_processes
    symbol_chunks = [
        nse_list_final[i : i + chunk_size]
        for i in range(0, len(nse_list_final), chunk_size)
    ]

    # Create a multiprocessing Pool to run the processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(process_symbols, symbol_chunks)

    # Concatenate the results from each process
    main_stock_dfs = results
    main_stock_df = pd.concat(main_stock_dfs, ignore_index=True)
    main_stock_df.to_csv("main_stock_df.csv")
