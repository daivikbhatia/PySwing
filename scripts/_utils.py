#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/swing_trading
#
import traceback
import talib
import pandas as pd
from datetime import datetime, timedelta
pd.options.mode.chained_assignment


class DataHandler:
    """
    This class contains all the helper functions
    needed by different scripts in the pipeline.
    """
    def __init__(self):
        pass

    @staticmethod
    def log_error(e):
        """
        This functions logs the error messages.
        """
        tb = traceback.TracebackException.from_exception(e)
        error_details = {
            "file_name": tb.stack[-1].filename,
            "line_number": tb.stack[-1].lineno,
            "error_message": str(e),
            "stack_trace": ''.join(tb.format())
        }
        print(f"Error in file '{error_details['file_name']}', line {error_details['line_number']}: {error_details['error_message']}")
        print(f"Stack trace:\n{error_details['stack_trace']}")
    
    @staticmethod
    def symbol_maker(stocks_file):
        """
        This function takes a csv file containing stock symbol names as input (.NS for NSE).
        It adds a suffix at end of each symbol. 
        This suffix helps yfinance to identify stock's category (NSE, BSE, etc)
        """
        nse = pd.read_csv(stocks_file)
        nse_list = list(nse["Symbol"])
        nse_list_final = []

        for i in nse_list:
            i = str(i)
            j = i + ".NS"
            nse_list_final.append(j)
        return nse_list_final
    
    @staticmethod
    def preprocess_data(df):
        """
        This functions convert Technical indicator's value to float.
        """
        df = df.dropna()
        df["is_5_rising"] = df["is_5_rising"].astype(float)
        df["is_20_rising"] = df["is_20_rising"].astype(float)
        return df

    @staticmethod
    def split_date(date_str):
        """
        This function splits the date.
        """
        date = str(date_str).split()[0]
        return pd.to_datetime(date)
    
    @staticmethod
    def calculate_week_and_day(df):
        """
        This functions takes df as input and process the date column by
        converting date to dateTime object, adding week number (week of the year)
        and day number (day of the week).

        """
        df["date"] = pd.to_datetime(df["date"])
        df["week_number"] = df["date"].apply(lambda x: x.isocalendar()[1])
        df["day_number"] = df["date"].apply(lambda x: x.isocalendar()[2])
        return df
    
    @staticmethod
    def date_20_days_from_now():
        """
        This functions return a date that was 20 days from today.
        """
        today = datetime.today()
        past_date = today - timedelta(days=20)
        return past_date.strftime('%Y-%m-%d')
    
    @staticmethod
    def add_hpt(temp):
        """
        This function combines sma and ema to a list.
        """
        temp["hpt_sma"] = "empty"
        temp.reset_index(drop=True, inplace=True)
        for index, row in temp.iterrows():
            hpt = []
            hpt.append(row["sma"])
            hpt.append(row["ema"])
            temp.loc[index, "hpt_sma"] = str(hpt)
        return temp
    
    @staticmethod
    def sma_builder(x):

        y = []
        x = eval(x)
        y.append(x[0])
        y.append(x[1])
        return str(y)
    
    @staticmethod
    def reset_dataFrames():
        """
        This functions deletes all the previous data in the dataFrames of data/processed directory
        """
        pd.DataFrame(columns= ['week_number', 'stockName', 'date', 'budget', 'loss_affordable',
       'number_of_loses', 'per_trade_loss', 'stock_entry', 'stop_loss',
       'rupee_stop', 'quantity', 'target', 'total_buy_amount',
       'total_buy_with_leverage', 'total_profit', 'new_target', 'result',
       'result_close_price', 'stopLoss_list', 'exit_time', 'day_number',
       'time', 'exit_result', 'day', 'hpt_list']).to_csv("../data/processed/main_buyit.csv")

        pd.DataFrame(columns= ['total_income', 'pl_ratio', 'profit_count', 'loss_count', 'exit_count',
       'sma', 'ema', 'sma_len', 'ema_len', 'atr_rot', 'year']).to_csv("../data/processed/investment_results.csv")

        pd.DataFrame(columns= ['week_number', 'stockName', 'date', 'budget', 'loss_affordable',
       'number_of_loses', 'per_trade_loss', 'stock_entry', 'stop_loss',
       'rupee_stop', 'quantity', 'target', 'total_buy_amount',
       'total_buy_with_leverage', 'total_profit', 'new_target', 'result',
       'result_close_price', 'stopLoss_list', 'exit_time', 'day_number',
       'time', 'exit_result', 'day', 'total_income', 'pl_ratio',
       'profit_count', 'loss_count', 'exit_count', 'current_month', 'hpts',
       'year']).to_csv("../data/processed/backtest_monthly.csv")    


class Strategy(DataHandler):
    """
    This class contains all the Technical analysis strategies.
    """
    def __init__(self, sma, ema, atr_rot, sma_len, ema_len, before_co):
        super().__init__()
        self.sma = sma
        self.ema = ema
        self.atr_rot = atr_rot
        self.sma_len = sma_len
        self.ema_len = ema_len
        self.before_co = before_co


    def moving_average_crossover_signal(self, MA_44, MA_200, window=4, percentage_margin=0.015):
        """
        If the output of this function is true then there is a crossover about to happen
        or has recently happened for the input stock on the input moving averages.
        """
        try:
            crossover_signal = False 
            #print("this is BEFORE CO",self.before_co)
            if self.before_co == True:
                stock_price = (MA_44 + MA_200) / 2.0
                margin = percentage_margin * stock_price
                for i in range(1, window + 1):
                    if (
                        MA_44.iloc[-i] + margin.iloc[-i] > MA_200.iloc[-i]
                        and MA_44.iloc[-i - 1] <= MA_200.iloc[-i - 1]
                    ):
                        crossover_signal = True 
                        break  
            elif self.before_co == False:
                for i in range(1, window + 1):
                    if (
                        MA_44.iloc[-i] > MA_200.iloc[-i]
                        and MA_44.iloc[-i - 1] <= MA_200.iloc[-i - 1]
                    ):
                        crossover_signal = (
                            True 
                        )
                        break  
        except Exception as e:
            DataHandler.log_error(e)

        return crossover_signal
    
    @staticmethod
    def adx_signal(data):
        """
        This function calculates the ADX for the given stock.
        """
        try:
            adx = talib.ADX(data["High"], data["Low"], data["Close"], timeperiod=14)

        except Exception as e:
            DataHandler.log_error(e)       

        return adx.iloc[-1] > 22
    
    @staticmethod
    def di_signals(data, buy_flag=True):
        """
        This function calculates the DI signals for the given stock.
        """
        try:
            plus_di = talib.PLUS_DI(data["High"], data["Low"], data["Close"], timeperiod=14)
            minus_di = talib.MINUS_DI(data["High"], data["Low"], data["Close"], timeperiod=14)
            if buy_flag:
                return plus_di.iloc[-1] > 22 and minus_di.iloc[-1] < plus_di.iloc[-1]
            else:
                return minus_di.iloc[-1] > 22 and minus_di.iloc[-1] > plus_di.iloc[-1]
        except Exception as e:
            DataHandler.log_error(e)

    @staticmethod    
    def split_date(x):
        """
        This function splits the date.
        """
        x = str(x)
        date = x.split()[0]
        date = pd.to_datetime(date)
        return date
    
    @staticmethod
    def is_always_falling_with_small_lows(lis, rot_ind):
        """
        If the output of this function is true then the moving average is falling.
        """
        try:
            for i in range(1, len(lis)):
                if lis[i] > lis[i - 1]:
                    return False
            return True
        except Exception as e:
            DataHandler.log_error(e)
    
    @staticmethod
    def is_always_rising_with_small_lows(data, lis, base, rot_ind=0.01):
        """
        If the output of this function is true then the moving average is rising.
        """
        try:
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
        except Exception as e:
            DataHandler.log_error(e)
    
    def investment(self,df,atr_rot):
        """
        This function adds all the investment columns to the dataFrame.
        """
        try:
            df["stockName"] = df["ticker_name"]
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
                        "stockName",
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
                        "stockName",
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
        
        except Exception as e:
            DataHandler.log_error(e)