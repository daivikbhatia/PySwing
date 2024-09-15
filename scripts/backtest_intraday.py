#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/PySwing
#
import pandas as pd
import warnings
from _utils import DataHandler, Strategy
import talib

warnings.filterwarnings("ignore")


class BackTestEngine(Strategy, DataHandler):
    """
    This class contains the entire backtesting code.
    """
    def __init__(
        self, start_date, sma, ema, atr_rot, sma_len, ema_len, before_co, df, final_df
    ):
        super().__init__(sma, ema, atr_rot, sma_len, ema_len, before_co)

        self.start_date = start_date
        self.df = df
        self.final_df = final_df

    def run_bactTest(self):
        """
        This function runs the main backtesting
        """
        try:
            self.final_df = self.preprocess_data(self.final_df)
            buy_filter_df = self.final_df.loc[
                (self.final_df["is_5_rising"] >= 0.8)
                & (self.final_df["is_20_rising"] >= 0.8)
                & (self.final_df["crossover"] == True)
            ]
            finalBuy_df = self.investment(buy_filter_df, self.atr_rot)
            finalBuy_df = finalBuy_df.loc[
                pd.to_datetime(finalBuy_df["Date"]) >= pd.to_datetime(self.start_date)
            ]
            investment_results_df, monthly_df, finalBuy_df = self.calculate_results(
                finalBuy_df
            )

            self.save_results(investment_results_df, monthly_df, finalBuy_df)

        except Exception as e:
            DataHandler.log_error(e)

    def calculate_results(self, finalBuy_df):
        """
        This function calculates the results like past profit, loss, exits, etc.
        """
        try:
            self.df = self.df.rename(columns={"stock": "stockName", "Date": "date"})

            finalBuy_df = finalBuy_df.rename(columns={"Datetime": "date"})
            finalBuy_df.rename(columns={"Date": "date"}, inplace=True)
            finalBuy_df["new_target"] = finalBuy_df["stock_entry"] + (
                3 * finalBuy_df["rupee_stop"]
            )
            finalBuy_df["result"] = "empty"
            finalBuy_df["result_close_price"] = "empty"
            finalBuy_df["stopLoss_list"] = "empty"
            finalBuy_df["exit_time"] = "empty"
            finalBuy_df["date"] = pd.to_datetime(finalBuy_df["date"])
            finalBuy_df = self.calculate_week_and_day(finalBuy_df)
            finalBuy_df = (
                finalBuy_df.groupby(["week_number", "stockName"]).first().reset_index()
            )
            finalBuy_df = finalBuy_df.dropna()
            finalBuy_df["time"] = finalBuy_df["date"]

            count = 0
            for indexes, rows in finalBuy_df.iterrows():
                sl_list = []
                count = count + 1
                # print(count)
                next_df = self.df.loc[self.df["stockName"] == rows["stockName"]]
                next_df = next_df.drop(columns=["stockName"])
                the_date = rows["date"]
                the_date = str(the_date)
                next_df = next_df.set_index("date")
                next_df.index = pd.to_datetime(next_df.index)
                day_df = next_df.loc[pd.to_datetime(the_date) :]
                day_df["Previous_low"] = day_df["Low"].shift(1)
                stopLoss = rows["stop_loss"]
                entry = rows["stock_entry"]
                target = rows["target"]
                trail = entry - stopLoss
                sl_list.append(stopLoss)
                for index, row in day_df.iterrows():
                    if row["Low"] > row["Previous_low"]:
                        sl = row["Low"] - trail
                        sl_list.append(sl)
                    stopLoss = max(sl_list)
                    if stopLoss >= row["Low"]:
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
                finalBuy_df.loc[indexes, "stopLoss_list"] = str(sl_list)
                finalBuy_df.loc[indexes, "result"] = result
                finalBuy_df.loc[indexes, "result_close_price"] = candle_close
                finalBuy_df.loc[indexes, "exit_time"] = exit_time

            finalBuy_df["exit_result"] = finalBuy_df["quantity"] * (
                finalBuy_df["result_close_price"] - finalBuy_df["stock_entry"]
            )
            loss_temp = finalBuy_df.loc[finalBuy_df["result"] == "loss"]
            loss_money = loss_temp["exit_result"].sum()

            profit_temp = finalBuy_df.loc[finalBuy_df["result"] == "profit"]
            profit_money = profit_temp["exit_result"].sum()

            total_income_org = profit_money + loss_money
            pl_list = dict(finalBuy_df["result"].value_counts())
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
                "sma": self.sma,
                "ema": self.ema,
                "sma_len": self.sma_len,
                "ema_len": self.ema_len,
                "atr_rot": self.atr_rot,
            }
            investment_results_df = pd.DataFrame(paisa_dict, index=[0])
            investment_results_df["year"] = self.start_date

            finalBuy_df["day"] = finalBuy_df["date"].apply(self.split_date)

            monthly_df = pd.DataFrame()
            for current_month in range(1, 12):
                monthly_df = finalBuy_df.loc[
                    (
                        finalBuy_df["day"]
                        >= pd.to_datetime(f"2023-{str(current_month)}-01")
                    )
                    & (
                        finalBuy_df["day"]
                        <= pd.to_datetime(f"2023-{str(current_month + 1)}-01")
                    )
                ]
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
                accounting_df = pd.DataFrame(paisa_m_dict, index=[0])
                accounting_df["current_month"] = current_month
                monthly_df = pd.concat([monthly_df, accounting_df])

            monthly_df["hpts"] = str(
                [self.sma, self.ema, self.sma_len, self.ema_len, self.atr_rot]
            )
            monthly_df["year"] = self.start_date
            list_hpt = [self.sma, self.ema, self.sma_len, self.ema_len, self.atr_rot]
            finalBuy_df["hpt_list"] = str(list_hpt)

        except Exception as e:
            DataHandler.log_error(e)

        return investment_results_df, monthly_df, finalBuy_df

    def save_results(self, investment_results_df, monthly_df, finalBuy_df):
        """
        This function saves the backtesting tables to the data/processed folder.
        """
        try:
            investment_results_df.to_csv(
                "../data/processed/investment_results.csv", mode="a", header=False
            )
            monthly_df.to_csv(
                "../data/processed/backtest_monthly.csv", mode="a", header=False
            )
            finalBuy_df.to_csv(
                "../data/processed/main_buyit.csv", mode="a", header=False
            )

        except Exception as e:
            DataHandler.log_error(e)


class TradingEngine(Strategy):
    """
    This is the main trading engine class that performs
    all the technical analysis on stocks input
    """
    def __init__(
        self, sma, ema, atr_rot, sma_len, ema_len, before_co, nse_list_final, mains_df
    ):
        super().__init__(sma, ema, atr_rot, sma_len, ema_len, before_co)
        self.nse_list_final = nse_list_final
        self.mains_df = mains_df

    def main_engine(self):
        """
        This is the main trading function.
        """
        try:
            all_df = pd.DataFrame()
            for ticker in self.nse_list_final:
                next_df = self.mains_df.loc[self.mains_df["stock"] == ticker]
                MA_44 = talib.MA(next_df["Close"], timeperiod=self.sma)

                MA_5 = talib.MA(next_df["Close"], timeperiod=self.ema)
                atr = talib.ATR(
                    next_df["High"], next_df["Low"], next_df["Close"], timeperiod=14
                )

                inter_df = pd.DataFrame()
                next_df = next_df.reset_index()
                next_df = next_df.rename(columns={"Datetime": "date"})

                for index in range(50, next_df.shape[0]):
                    next_df_ = next_df[:index]
                    temp_df = next_df.iloc[index - 1 : index]
                    MA_44_ = MA_44[:index]
                    MA_5_ = MA_5[:index]
                    atr_ = atr[:index]
                    temp_df["ticker_name"] = ticker
                    temp_df["is_20_rising"] = self.is_always_rising_with_small_lows(
                        next_df_[-self.sma_len :],
                        list(MA_44_[-self.sma_len :]),
                        base=self.sma_len,
                        rot_ind=0.01,
                    )
                    temp_df["is_5_rising"] = self.is_always_rising_with_small_lows(
                        next_df_[-self.ema_len :],
                        list(MA_5_[-self.ema_len :]),
                        base=self.ema_len,
                        rot_ind=0.01,
                    )
                    temp_df["atr_value"] = list(atr_[-1:])[0]
                    temp_df["crossover"] = self.moving_average_crossover_signal(
                        MA_44_[-50:], MA_5_[-50:]
                    )
                    inter_df = pd.concat([inter_df, temp_df])

                all_df = pd.concat([all_df, inter_df])

        except Exception as e:
            DataHandler.log_error(e)
            pass
        return all_df
