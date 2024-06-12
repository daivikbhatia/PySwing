import pandas as pd
from backtest_intraday import *


def backtest_engine_fn(pd, sma, ema):
    try:
        df = pd.read_csv("./main_stock_df.csv", index_col=0)
        all_df = pd.read_csv("./bt_all_processed_date.csv", index_col=0)
        df = df.rename(columns={"stock": "stockname"})
        final_df = all_df
        buy_filter_df = final_df.loc[
            (final_df["is_5_rising"] >= 0.8)
            # &(final_df["is_above_44_rising"] == True)
            & (final_df["is_20_rising"] >= 0.8)
            & (final_df["crossover"] == True)
            # &(final_df["crossover_2"] == True)
            # &(final_df["Is_Green_Candle"] == True)
            # &(final_df["is_close_to_44_rising"] == True)
            # &(final_df["is_big_candle"] == True)
        ]
        finalbuy_df = investment(buy_filter_df, buy_flag=True)
        finalbuy_df = finalbuy_df.rename(columns={"Datetime": "date"})
        finalbuy_df.rename(columns={"Date": "date"}, inplace=True)
        df.rename(columns={"Date": "date"}, inplace=True)

        import pandas as pd

        # finalbuy_df = pd.read_csv("./finalbuy_res.csv",index_col=0)
        # finalbuy_df = invest_df
        finalbuy_df["new_target"] = finalbuy_df["stock_entry"] + (
            3 * finalbuy_df["rupee_stop"]
        )
        finalbuy_df["result"] = "empty"
        finalbuy_df["result_close_price"] = "empty"
        finalbuy_df["exit_time"] = "empty"
        finalbuy_df["date"] = pd.to_datetime(finalbuy_df["date"])
        finalbuy_df["week_number"] = finalbuy_df["date"].apply(
            lambda x: x.isocalendar()[1]
        )
        finalbuy_df["day_number"] = finalbuy_df["date"].apply(
            lambda x: x.isocalendar()[2]
        )
        finalbuy_df = (
            finalbuy_df.groupby(["week_number", "day_number", "stockname"])
            .first()
            .reset_index()
        )
        finalbuy_df = finalbuy_df.dropna()

        # finalbuy_df = finalbuy_df.loc[finalbuy_df["stockname"].isin(nifty_50_lis)]
        def stripit(x):
            time = x.split(" ")[1]
            time = pd.to_datetime(time)
            return time

        finalbuy_df["time"] = finalbuy_df["date"]
        # finalbuy_df = finalbuy_df.loc[pd.to_datetime(finalbuy_df["time"]) >= pd.to_datetime("9:30:00+05:30")]
        # finalbuy_df = finalbuy_df.loc[finalbuy_df["time"] <= pd.to_datetime("11:00:00")]
        count = 0
        for indexs, rows in finalbuy_df.iterrows():
            count = count + 1
            # print(count)
            next_df = df.loc[df["stockname"] == rows["stockname"]]
            next_df = next_df.drop(columns=["stockname"])
            the_date = rows["date"]
            the_date = str(the_date)
            striped_date = the_date.split(" ")[0]
            next_df = next_df.set_index("date")
            next_df.index = pd.to_datetime(next_df.index)
            day_df = next_df.loc[pd.to_datetime(the_date) :]
            stoploss = rows["stop_loss"]
            target = rows["target"]
            for index, row in day_df.iterrows():
                if stoploss >= row["Low"]:
                    result = "loss"
                    candle_close = row["Close"]
                    exit_time = index
                    break
                elif target <= row["High"]:
                    result = "profit"
                    candle_close = row["Close"]
                    exit_time = index
                    break
                else:
                    result = "exit"
                    candle_close = row["Close"]
                    exit_time = index

            finalbuy_df.loc[indexs, "result"] = result
            finalbuy_df.loc[indexs, "result_close_price"] = candle_close
            finalbuy_df.loc[indexs, "exit_time"] = exit_time

        temp = finalbuy_df.loc[finalbuy_df["result"] == "exit"]
        temp["exit_result"] = (temp["quantity"] * temp["result_close_price"]) - (
            temp["quantity"] * temp["stock_entry"]
        )
        temp = temp.loc[(temp["exit_result"] <= 2000) & (temp["exit_result"] >= -1000)]
        exit_money = temp["exit_result"].sum()

        loss_money = finalbuy_df.loc[finalbuy_df["result"] == "loss"]
        loss_money = loss_money.shape[0] * 1000

        profit_money = finalbuy_df.loc[finalbuy_df["result"] == "profit"]
        profit_money = profit_money.shape[0] * 2000

        total_income_org = profit_money - loss_money
        pl_list = dict(finalbuy_df["result"].value_counts())
        pl_ratio = pl_list["profit"] / (pl_list["profit"] + pl_list["loss"])
        paisa_dict = {
            "total_income": total_income_org,
            "pl_ratio": pl_ratio,
            "profit_count": pl_list["profit"],
            "loss_count": pl_list["loss"],
            "exit_count": pl_list["exit"],
            "sma": sma,
            "ema": ema,
        }
        paisabnaya_df = pd.DataFrame(paisa_dict, index=[0])
        paisabnaya_df.to_csv("./paisabnaya_results.csv", mode="a", header=False)

    except:
        pl_list = dict(finalbuy_df["result"].value_counts())
        pl_ratio = pl_list["profit"] / (pl_list["profit"] + pl_list["loss"])
        paisa_dict = {
            "total_income": total_income_org,
            "pl_ratio": pl_ratio,
            "profit_count": pl_list["profit"],
            "loss_count": pl_list["loss"],
            "exit_count": 0,
            "sma": sma,
            "ema": ema,
        }
        paisabnaya_df = pd.DataFrame(paisa_dict, index=[0])
        paisabnaya_df.to_csv("./paisabnaya_results.csv", mode="a", header=False)

    return finalbuy_df
