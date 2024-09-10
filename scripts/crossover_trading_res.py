#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/swing_trading
#
import pandas as pd
import yfinance as yf
import datetime as dt
import dataframe_image as dfi
from _utils import DataHandler


class FinalProcessing(DataHandler):
    """
    This class takes the output of daily_crossover and converts it into a final readable image.
    """
    def __init__(self, buy_df, main_df, allTime_buy, st_1, god_df, stock_df, fun_df):
        super().__init__()
        self.buy_df = buy_df
        self.main_df = main_df
        self.allTime_buy = allTime_buy
        self.st_1 = st_1
        self.god_df = god_df
        self.stock_df = stock_df
        self.fun_df = fun_df
        self.today = DataHandler.date_20_days_from_now()

    def get_net_change(self, df):
        """
        This function defines how the industry has changed in last 20 days.
        """
        df = df.merge(self.stock_df, on="stock", how="outer")
        df["net_change"] = "empty"
        df = DataHandler.net_change_fn(df)
        change_df = (
            df.groupby(["Industry", "Date"])
            .mean()
            .reset_index()[["Industry", "Date", "Close", "net_change"]]
        )
        change_df["Date"] = pd.to_datetime(change_df["Date"])
        return change_df

    def backTest_analysis_result(self, stock, old_date):
        """
        This function does all the post processing on the output data
        and converts the final result to a dataFrame.
        """
        change_df = self.get_net_change(self.main_df)
        strat_count = self.buy_df.loc[self.buy_df["stockName"] == stock].shape[0]
        current_smas = str(
            list(self.buy_df.loc[self.buy_df["stockName"] == stock]["smas"])
        )
        temps = self.allTime_buy.loc[self.allTime_buy["stockName"] == stock]
        self.stock_df = pd.DataFrame()
        for i in list(self.buy_df.loc[self.buy_df["stockName"] == stock]["hpt_list"]):
            mini = temps.loc[temps["hpt_list"] == i]
            self.stock_df = pd.concat([self.stock_df, mini])
        strat_result_count = str(dict(self.stock_df["result"].value_counts()))
        strat_result_mean = self.stock_df["exit_result"].mean()
        stock_industry = list(
            self.buy_df.loc[self.buy_df["stockName"] == stock]["Industry"]
        )[0]
        strat_test = temps.loc[temps["hpt_list"].isin(list(self.st_1["hpt_list"]))]
        stock_result_count = str(dict(strat_test["result"].value_counts()))
        stock_result_mean = strat_test["exit_result"].mean()
        investment_df = self.buy_df.loc[
            (self.buy_df["stockName"] == stock)
            & (self.buy_df["date"] == self.buy_df["date"].max())
        ]
        try:
            entry = list(investment_df["stock_entry"])[0]
            stopLoss = list(investment_df["stop_loss"])[0]
            rupee_stop = list(investment_df["rupee_stop"])[0]
            quantity = list(investment_df["quantity"])[0]
            buy_amount = list(investment_df["total_buy_amount"])[0]
            investment_list = [entry, stopLoss, rupee_stop, quantity, buy_amount]

            old_price = list(
                change_df.loc[
                    (change_df["Industry"] == stock_industry)
                    & (change_df["Date"] >= old_date)
                ]["Close"]
            )[0]
            new_price = list(
                change_df.loc[
                    (change_df["Industry"] == stock_industry)
                    & (change_df["Date"] == change_df["Date"].max())
                ]["Close"]
            )[0]
            net_change = ((new_price - old_price) / old_price) * 100
        except:
            net_change = 0
            investment_list = 0
        decision_dict = {
            "stockName": stock,
            "stock_industry": stock_industry,
            "net_industry_change": net_change,
            "strat_count": strat_count,
            "current_smas": current_smas,
            "stat_res_count_stock": strat_result_count,
            "stat_res_mean_stock": strat_result_mean,
            "stock_res_count_overall": stock_result_count,
            "stock_res_mean_overall": stock_result_mean,
            "investment_data": str(investment_list),
        }
        decision_df = pd.DataFrame(decision_dict, index=[0])
        return decision_df

    def main(self):
        """
        This function does all the post processing on the output data
        and converts the final result to an image data/processed/crossover_table.png.
        """
        self.buy_df["smas"] = self.buy_df["hpt_list"].apply(DataHandler.sma_builder)
        self.buy_df = self.buy_df.sort_values(["stockName", "smas"])
        self.buy_df = self.buy_df.groupby(["stockName", "smas"]).first().reset_index()
        self.buy_df = self.buy_df.sort_values(["stockName", "date"], ascending=False)

        self.stock_df.rename(columns={"stock": "stockName"}, inplace=True)
        self.buy_df = self.buy_df.merge(self.stock_df, on="stockName", how="inner")

        recent_stocks = list(
            set(list(self.buy_df.loc[self.buy_df["date"] >= self.today]["stockName"]))
        )

        self.st_1 = self.st_1.rename(columns={"new_hpt_lis": "hpt_list"})
        self.buy_df["date"] = pd.to_datetime(self.buy_df["date"])
        hptss_list = []
        for i in self.st_1["hpt_list"]:
            hpt = eval(i)
            hpt.pop()
            hptss_list.append(str(hpt))
        self.st_1["hpt_list"] = hptss_list

        self.god_df = DataHandler.add_hpt(self.god_df)
        self.god_df = self.god_df.loc[self.god_df["year"] != "2023-08-01"]

        final_decision_df = pd.DataFrame()
        for name in list(self.buy_df["stockName"].unique()):
            temp_df = self.backTest_analysis_result(name, "2023-11-24")
            rat_list = []
            for i in eval(list(temp_df["current_smas"])[0]):
                pl_df = self.god_df.loc[self.god_df["hpt_sma"] == i]
                pl_rat = pl_df["pl_ratio"].mean()
                pl_rat = round(pl_rat, 3)
                rat_list.append(pl_rat)
            rat_list = str(rat_list)
            temp_df["pl_ratio_list"] = rat_list
            final_decision_df = pd.concat([final_decision_df, temp_df])
            final_decision_df = final_decision_df[
                [
                    "stockName",
                    "stock_industry",
                    "net_industry_change",
                    "strat_count",
                    "current_smas",
                    "pl_ratio_list",
                    "stat_res_count_stock",
                    "stat_res_mean_stock",
                    "stock_res_count_overall",
                    "stock_res_mean_overall",
                    "investment_data",
                ]
            ]

        recent_stocks = list(
            set(list(self.buy_df.loc[self.buy_df["date"] >= self.today]["stockName"]))
        )
        show_df = final_decision_df.loc[
            final_decision_df["stockName"].isin(recent_stocks)
        ].sort_values("strat_count", ascending=False)
        show_df = show_df.reset_index()
        show_df = show_df.merge(self.fun_df, on="stockName")
        show_df = show_df.drop(columns=["current_smas", "pl_ratio_list"])
        today_date = str(dt.date.today())
        show_df["run_date"] = today_date
        show_df.to_csv("../data/processed/final_crossover_df.csv")
        df_styled = show_df.style.background_gradient()
        dfi.export(df_styled, "../data/processed/crossover_table.png")


if __name__ == "__main__":
    buy_df = pd.read_csv("../data/processed/main_buyit.csv", index_col=0)
    main_df = pd.read_csv("../data/processed/main_stock_df.csv", index_col=0)
    stock_df = pd.read_csv("../data/input/sectordf.csv", index_col=0)
    allTime_buy = pd.read_csv("../data/input/main_buyit_jan_2024.csv", index_col=0)
    st_1 = pd.read_csv("../data/input/final_stratergy_jan_2024.csv", index_col=0)
    god_df = pd.read_csv("../data/input/investment_results_jan_2024.csv", index_col=0)
    fun_df = pd.read_csv("../data/processed/fundamentals_res.csv", index_col=0)

    final_process = FinalProcessing(
        buy_df, main_df, allTime_buy, st_1, god_df, stock_df, fun_df
    )
    final_process.main()
