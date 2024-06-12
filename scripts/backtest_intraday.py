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


def backtest_engine_fn(
    pd,
    all_df,
    sma,
    ema,
    sma_len,
    ema_len,
    atr_rot,
    start="2021-01-01",
    end="2022-01-01",
):
    # try:
    df = pd.read_csv("./main_stock_df.csv", index_col=0)
    # all_df = pd.read_csv("./bt_all_processed_date.csv",index_col=0)
    df = df.rename(columns={"stock": "stockname"})
    final_df = all_df
    final_df = final_df.dropna()
    final_df["is_5_rising"] = final_df["is_5_rising"].astype(float)
    final_df["is_20_rising"] = final_df["is_20_rising"].astype(float)
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
    finalbuy_df = investment(buy_filter_df, atr_rot, buy_flag=True)
    finalbuy_df = finalbuy_df.loc[
        pd.to_datetime(finalbuy_df["Date"]) >= pd.to_datetime(start)
    ]
    # finalbuy_df = finalbuy_df.loc[pd.to_datetime(finalbuy_df["Date"]) <= pd.to_datetime(end)]
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
    finalbuy_df["stoploss_list"] = "empty"
    finalbuy_df["exit_time"] = "empty"
    finalbuy_df["date"] = pd.to_datetime(finalbuy_df["date"])
    finalbuy_df["week_number"] = finalbuy_df["date"].apply(lambda x: x.isocalendar()[1])
    finalbuy_df["day_number"] = finalbuy_df["date"].apply(lambda x: x.isocalendar()[2])
    finalbuy_df = (
        finalbuy_df.groupby(["week_number", "stockname"]).first().reset_index()
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
        sl_list = []
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
        day_df["Previous_low"] = day_df["Low"].shift(1)
        stoploss = rows["stop_loss"]
        entry = rows["stock_entry"]
        target = rows["target"]
        trail = entry - stoploss
        sl_list.append(stoploss)
        for index, row in day_df.iterrows():
            if row["Low"] > row["Previous_low"]:
                sl = row["Low"] - trail
                sl_list.append(sl)
            stoploss = max(sl_list)
            if stoploss >= row["Low"]:
                candle_close = row["Close"]
                exit_time = index
                if candle_close >= entry:
                    result = "profit"
                else:
                    result = "loss"
                break
            else:
                result = "exit"
                candle_close = row["Close"]
                exit_time = index
        finalbuy_df.loc[indexs, "stoploss_list"] = str(sl_list)
        finalbuy_df.loc[indexs, "result"] = result
        finalbuy_df.loc[indexs, "result_close_price"] = candle_close
        finalbuy_df.loc[indexs, "exit_time"] = exit_time

    finalbuy_df["exit_result"] = finalbuy_df["quantity"] * (
        finalbuy_df["result_close_price"] - finalbuy_df["stock_entry"]
    )
    temp = finalbuy_df.loc[finalbuy_df["result"] == "exit"]

    # temp = temp.loc[(temp["exit_result"] <=2000) & (temp["exit_result"] >=-1000)]
    exit_money = temp["exit_result"].sum()

    loss_temp = finalbuy_df.loc[finalbuy_df["result"] == "loss"]
    loss_money = loss_temp["exit_result"].sum()

    profit_temp = finalbuy_df.loc[finalbuy_df["result"] == "profit"]
    profit_money = profit_temp["exit_result"].sum()

    total_income_org = profit_money + loss_money
    pl_list = dict(finalbuy_df["result"].value_counts())
    try:
        profits = pl_list["profit"]
    except:
        profits = 0
    try:
        losses = pl_list["loss"]
    except:
        losses = 0
    try:
        exits = pl_list["exit"]
    except:
        exits = 0
    if profits + losses == 0:
        pl_ratio = 0
    else:
        pl_ratio = profits / (profits + losses)
    paisa_dict = {
        "total_income": total_income_org,
        "pl_ratio": pl_ratio,
        "profit_count": profits,
        "loss_count": losses,
        "exit_count": exits,
        "sma": sma,
        "ema": ema,
        "sma_len": sma_len,
        "ema_len": ema_len,
        "atr_rot": atr_rot,
    }
    paisabnaya_df = pd.DataFrame(paisa_dict, index=[0])
    paisabnaya_df["year"] = start
    paisabnaya_df.to_csv("./paisabnaya_results.csv", mode="a", header=False)

    def split_date(x):
        x = str(x)
        date = x.split()[0]
        date = pd.to_datetime(date)
        return date

    finalbuy_df["day"] = finalbuy_df["date"].apply(split_date)
    galla_df = pd.DataFrame()
    for mahina in range(1, 12):
        monthly_df = finalbuy_df.loc[
            (finalbuy_df["day"] >= pd.to_datetime(f"2023-{str(mahina)}-01"))
            & (finalbuy_df["day"] <= pd.to_datetime(f"2023-{str(mahina + 1)}-01"))
        ]
        # temp = monthly_df.loc[monthly_df["result"] == "exit"]
        # temp["exit_result"] = (temp["quantity"] * temp["result_close_price"]) - (temp["quantity"] * temp["stock_entry"])
        # temp = temp.loc[(temp["exit_result"] <=10000) & (temp["exit_result"] >=-5000)]
        # exit_money = temp["exit_result"].sum()

        l_money = monthly_df.loc[monthly_df["result"] == "loss"]
        l_money = l_money["exit_result"].sum()

        p_money = monthly_df.loc[monthly_df["result"] == "profit"]
        p_money = p_money["exit_result"].sum()

        total_income_m = p_money - l_money
        pl_list = dict(monthly_df["result"].value_counts())
        try:
            profits = pl_list["profit"]
        except:
            profits = 0
        try:
            losses = pl_list["loss"]
        except:
            losses = 0
        try:
            exits = pl_list["exit"]
        except:
            exits = 0
        if profits + losses == 0:
            pl_ratio = 0
        else:
            pl_ratio = profits / (profits + losses)
        paisa_m_dict = {
            "total_income": total_income_m,
            "pl_ratio": pl_ratio,
            "profit_count": profits,
            "loss_count": losses,
            "exit_count": exits,
        }
        hisab_df = pd.DataFrame(paisa_m_dict, index=[0])
        hisab_df["mahina"] = mahina
        galla_df = pd.concat([galla_df, hisab_df])

    galla_df["hpts"] = str([sma, ema, sma_len, ema_len, atr_rot])
    galla_df["year"] = start
    galla_df.to_csv("./backtest_galla.csv", mode="a", header=False)
    # except:
    #    if profits + losses == 0:
    #        pl_ratio = 0
    #    else:
    #        pl_ratio = profits / (profits + losses)
    #    paisa_dict = {
    #        "total_income" : total_income_org,
    #        "pl_ratio" : pl_ratio,
    #        "profit_count" : profits,
    #        "loss_count" : losses,
    #        "exit_count" : exits,
    #        "sma" : sma,
    #        "ema" :ema,
    #        "sma_len" : sma_len,
    #        "ema_len" : ema_len
    #    }
    #    paisabnaya_df = pd.DataFrame(paisa_dict,index=[0])
    #    paisabnaya_df.to_csv("./paisabnaya_results.csv",mode="a",header=False)
    #    pass
    list_hpt = [sma, ema, sma_len, ema_len, atr_rot]
    finalbuy_df["hpt_list"] = str(list_hpt)
    finalbuy_df.to_csv("main_buyit.csv", mode="a", header=False)
    return finalbuy_df, paisabnaya_df, galla_df


def is_always_rising_with_small_lows(data, lis, base, rot_ind=0.01):
    # rot = (max(lis) - min(lis)) * rot_ind
    fin_res = 0
    rising_array = []
    for i in range(1, len(lis)):
        if lis[i] > lis[i - 1]:
            rising_array.append(1)
        else:
            rising_array.append(0)
    rising_avg = sum(rising_array) / len(rising_array)
    if rising_array[-1] == 0:
        fin_res = 0
    elif rising_array[-2] == 0:
        fin_res = 0
    else:
        fin_res = rising_avg

    return float(fin_res)


# xd = base
# yd = lis[-1] - lis[0]
# if yd / xd <= 0.57:
#    return False


"""def di_signals(data,buy_flag=True):
    # Ensure that the input data has 'High', 'Low', and 'Close' columns
    if not all(col in data.columns for col in ['High', 'Low', 'Close']):
        raise ValueError("Input data must have 'High', 'Low', and 'Close' columns")

    # Calculate +DI and -DI using TA-Lib
    plus_di = talib.PLUS_DI(data['High'], data['Low'], data['Close'], timeperiod=8)
    minus_di = talib.MINUS_DI(data['High'], data['Low'], data['Close'], timeperiod=8)

    # Check if +DI is above 20 and -DI is below 20
    if buy_flag == True:
        if plus_di.iloc[-1] > 20 and minus_di.iloc[-1] < 20:
            return True
        else:
            return False
    else:
        if plus_di.iloc[-1] < 20 and minus_di.iloc[-1] > 20:
            return True
        else:
            return False"""


def is_always_falling_with_small_lows(lis, rot_ind):
    # rot = (max(lis) - min(lis)) * rot_ind
    for i in range(1, len(lis)):
        if lis[i] > lis[i - 1]:
            return False
    return True


"""def moving_average_crossover_signal(MA_44, MA_200, window=4):
    crossover_signal = False  # Initialize the crossover signal as 0

    # Check for a crossover condition within the specified window
    for i in range(1, window + 1):
        if (
            MA_44.iloc[-i] > MA_200.iloc[-i]
            and MA_44.iloc[-i - 1] <= MA_200.iloc[-i - 1]
        ):
            crossover_signal = True  # Set the crossover signal to 1 if crossover condition is met
            break  # Exit the loop once a crossover is found within the window

    return crossover_signal"""


def moving_average_crossover_signal(
    before_co, MA_44, MA_200, window=4, percentage_margin=0.01
):
    crossover_signal = False  # Initialize the crossover signal as False

    if before_co == True:
        # Calculate the stock price as the average of the two moving averages
        stock_price = (MA_44 + MA_200) / 2.0
        margin = percentage_margin * stock_price
        # Calculate the margin based on the percentage of the stock price

        # Check for a potential crossover condition within the specified window
        for i in range(1, window + 1):
            # Check if the difference between MA_44 and MA_200 is less than or equal to the margin
            if (
                MA_44.iloc[-i] + margin.iloc[-i] > MA_200.iloc[-i]
                and MA_44.iloc[-i - 1] <= MA_200.iloc[-i - 1]
            ):
                crossover_signal = True  # Set the crossover signal to True if potential crossover condition is met
                break  # Exit the loop once a potential crossover is found within the window
    elif before_co == False:
        # Check for a crossover condition within the specified window
        for i in range(1, window + 1):
            if (
                MA_44.iloc[-i] > MA_200.iloc[-i]
                and MA_44.iloc[-i - 1] <= MA_200.iloc[-i - 1]
            ):
                crossover_signal = (
                    True  # Set the crossover signal to 1 if crossover condition is met
                )
                break  # Exit the loop once a crossover is found within the window

    return crossover_signal


# Example usage:
# signal = imminent_crossover_signal(MA_44, MA_200, window=4, percentage_margin=0.01)


def adx_signal(data):
    # Ensure that the input data has 'High', 'Low', and 'Close' columns
    if not all(col in data.columns for col in ["High", "Low", "Close"]):
        raise ValueError("Input data must have 'High', 'Low', and 'Close' columns")

    # Calculate the ADX using TA-Lib
    adx = talib.ADX(data["High"], data["Low"], data["Close"], timeperiod=14)

    # Check if ADX is above 20 and rising
    if adx.iloc[-1] > 22:
        return True
    else:
        return False


def di_signals(data, buy_flag=True):
    # Ensure that the input data has 'High', 'Low', and 'Close' columns
    if not all(col in data.columns for col in ["High", "Low", "Close"]):
        raise ValueError("Input data must have 'High', 'Low', and 'Close' columns")

    # Calculate +DI and -DI using TA-Lib
    plus_di = talib.PLUS_DI(data["High"], data["Low"], data["Close"], timeperiod=14)
    minus_di = talib.MINUS_DI(data["High"], data["Low"], data["Close"], timeperiod=14)

    # Check if +DI is above 20 and -DI is below 20
    if buy_flag == True:
        if plus_di.iloc[-1] > 22 and minus_di.iloc[-1] < plus_di.iloc[-1]:
            return True
        else:
            return False
    else:
        if minus_di.iloc[-1] > 22 and minus_di.iloc[-1] > plus_di.iloc[-1]:
            return True
        else:
            return False


def identify_big_candles(the_data):
    # Calculate the candle body size as the absolute difference between open and close prices
    data = the_data[-10:]
    data["CandleBodySize"] = abs(data["Open"] - data["Close"])

    # Calculate the sum of body sizes of the last four candles for each candle
    data["SumLast4Candles"] = (
        data["CandleBodySize"].rolling(window=4).sum() - data["CandleBodySize"]
    )

    # Identify big candles where the current candle's body size is greater than the sum of the last four candles
    data["IsBigCandle"] = data["CandleBodySize"] > data["SumLast4Candles"]

    for i in range(8, len(data)):
        current_candle = data.iloc[i]
        previous_candles = data.iloc[i - 8 : i]

        # Calculate the range (difference between high and low) for the current candle
        current_range = abs(current_candle["Close"] - current_candle["Open"])

        # Calculate the sum of ranges for the last 8 candles
        sum_of_ranges = previous_candles["High"].max() - previous_candles["Low"].min()

    if current_range > sum_of_ranges:
        return True

    elif list(data["IsBigCandle"][-1:])[0] == True:
        return True
    else:
        return False


def investment(df, atr_rot, buy_flag=True):
    if buy_flag == True:
        df["stockname"] = df["ticker_name"]
        df["budget"] = 45000
        df["loss_affordable"] = 10000
        df["number_of_loses"] = 10
        df["per_trade_loss"] = df["loss_affordable"] / df["number_of_loses"]
        df["stock_entry"] = df["Close"]
        df["stop_loss"] = df["Low"] - (atr_rot * df["atr_value"])
        df["rupee_stop"] = df["stock_entry"] - df["stop_loss"]
        df["quantity"] = df["per_trade_loss"] / df["rupee_stop"]
        df["target"] = df["stock_entry"] + (2 * df["rupee_stop"])
        df["total_buy_amount"] = df["stock_entry"] * df["quantity"]
        df["total_buy_with_leverage"] = df["total_buy_amount"] / 5
        df["total_profit"] = (df["target"] - df["stock_entry"]) * df["quantity"]

        try:
            result_df = df[
                [
                    "Date",
                    "stockname",
                    "budget",
                    "loss_affordable",
                    "number_of_loses",
                    "per_trade_loss",
                    "stock_entry",
                    "stop_loss",
                    "rupee_stop",
                    "quantity",
                    "target",
                    "total_buy_amount",
                    "total_buy_with_leverage",
                    "total_profit",
                ]
            ]
        except:
            result_df = df[
                [
                    "date",
                    "stockname",
                    "budget",
                    "loss_affordable",
                    "number_of_loses",
                    "per_trade_loss",
                    "stock_entry",
                    "stop_loss",
                    "rupee_stop",
                    "quantity",
                    "target",
                    "total_buy_amount",
                    "total_buy_with_leverage",
                    "total_profit",
                ]
            ]
    else:
        df["stockname"] = df["ticker_name"]
        df["budget"] = 45000
        df["loss_affordable"] = 10000
        df["number_of_loses"] = 20
        df["per_trade_loss"] = df["loss_affordable"] / df["number_of_loses"]
        df["stock_entry"] = df["Low"]
        df["stop_loss"] = df["High"]
        df["rupee_stop"] = df["stock_entry"] - df["stop_loss"]
        df["quantity"] = df["per_trade_loss"] / df["rupee_stop"]
        df["target"] = df["stock_entry"] + (2 * df["rupee_stop"])
        df["total_buy_amount"] = df["stock_entry"] * df["quantity"]
        df["total_buy_with_leverage"] = df["total_buy_amount"] / 5
        df["total_profit"] = (df["target"] - df["stock_entry"]) * df["quantity"]

        result_df = df[
            [
                "date",
                "stockname",
                "budget",
                "loss_affordable",
                "number_of_loses",
                "per_trade_loss",
                "stock_entry",
                "stop_loss",
                "rupee_stop",
                "quantity",
                "target",
                "total_buy_amount",
                "total_buy_with_leverage",
                "total_profit",
            ]
        ]
    return result_df


def trading_engine(before_co, hpt, mains_df, nse_list_final, start_date="2023-01-01"):
    sma = hpt[0]
    ema = hpt[1]
    sma_tail = hpt[2]
    ema_tail = hpt[3]
    # hehe = pd.read_csv("./indian_tickers.csv",index_col=0)
    # for i in hehe["Yahoo Stock Tickers"]:
    #    if ".NS" in i:
    #        nse_list_final.append(i)

    # main_stock_df = pd.DataFrame()
    all_df = pd.DataFrame()
    finalbuy_df = pd.DataFrame()
    count = 1
    total_time = time.time()
    try:
        for ticker in nse_list_final:
            stock_time = time.time()
            count = count + 1
            # next_df = yf.download(ticker,interval="1h",start='2023-01-01')
            next_df = mains_df.loc[mains_df["stock"] == ticker]
            MA_44 = talib.MA(next_df["Close"], timeperiod=sma)
            # MA_200 = talib.EMA(next_df['Close'], timeperiod=200)
            MA_5 = talib.MA(next_df["Close"], timeperiod=ema)
            atr = talib.ATR(
                next_df["High"], next_df["Low"], next_df["Close"], timeperiod=14
            )
            # print("stocks processed: ",count)
            inter_df = pd.DataFrame()
            next_df = next_df.reset_index()
            next_df = next_df.rename(columns={"Datetime": "date"})
            # next_df["stock"] = ticker
            # main_stock_df = pd.concat([main_stock_df,next_df])

            for index in range(50, next_df.shape[0]):
                next_df_ = next_df[:index]
                previous_df = next_df.iloc[index - 2 : index - 1]
                temp_df = next_df.iloc[index - 1 : index]
                MA_44_ = MA_44[:index]
                # MA_200_ = MA_200[:index]
                MA_5_ = MA_5[:index]
                atr_ = atr[:index]
                temp_df["ticker_name"] = ticker
                temp_df["is_20_rising"] = is_always_rising_with_small_lows(
                    next_df_[-sma_tail:],
                    list(MA_44_[-sma_tail:]),
                    base=sma_tail,
                    rot_ind=0.01,
                )
                # temp_df['Is_Green_Candle'] = temp_df['Close']  > temp_df['Open']
                # temp_df["is_above_44_rising"] = list(temp_df["Low"])[0] <= list(MA_44_[-1:])[0]
                # temp_df["is_close_to_44_rising"] = temp_df["Open"] +  ((temp_df["Close"] - temp_df["Open"]) / 4) >= list(MA_44_[-1:])[0]
                # temp_df["is_big_candle"] = identify_big_candles(next_df[:index])
                temp_df["is_5_rising"] = is_always_rising_with_small_lows(
                    next_df_[-ema_tail:],
                    list(MA_5_[-ema_tail:]),
                    base=ema_tail,
                    rot_ind=0.01,
                )
                # temp_df["44_value"] = list(MA_44_[-1:])[0]
                temp_df["atr_value"] = list(atr_[-1:])[0]
                temp_df["crossover"] = moving_average_crossover_signal(
                    before_co, MA_44_[-50:], MA_5_[-50:]
                )
                inter_df = pd.concat([inter_df, temp_df])

            all_df = pd.concat([all_df, inter_df])

    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno

        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
        pass
    return all_df, hpt
