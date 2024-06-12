import pandas as pd
import yfinance as yf
import datetime as dt
import dataframe_image as dfi


def date_20_days_from_now():
    today = dt.datetime.today()
    past_date = today - dt.timedelta(days=6)
    return past_date.strftime("%Y-%m-%d")


today = date_20_days_from_now()

df = pd.read_csv("./paisabnaya_results.csv", index_col=0)
buy_df = pd.read_csv("./main_buyit.csv", index_col=0)
mains_df = pd.read_csv("./main_stock_df.csv", index_col=0)


def sma_builder(x):
    y = []
    x = eval(x)
    y.append(x[0])
    y.append(x[1])
    return str(y)


def net_change_fn(df):
    retdf = pd.DataFrame()
    for stockname in list(df["stock"].unique()):
        tempdf = df.loc[df["stock"] == stockname]
        tempdf["net_change"] = tempdf["Close"].pct_change()
        retdf = pd.concat([retdf, tempdf])
    return retdf


main_df = pd.read_csv("./main_stock_df.csv", index_col=0)
df_ind = pd.read_csv("./sectordf.csv", index_col=0)


def get_net_change(df):
    df = df.merge(df_ind, on="stock", how="outer")
    df["net_change"] = "empty"
    df = net_change_fn(df)

    change_df = (
        df.groupby(["Industry", "Date"])
        .mean()
        .reset_index()[["Industry", "Date", "Close", "net_change"]]
    )
    change_df["Date"] = pd.to_datetime(change_df["Date"])
    # change_df20 = change_df.loc[change_df["Date"] >= pd.to_datetime("2023-11-19")]
    # change_df20 = change_df20.groupby("Industry").mean().reset_index()[["Industry","net_change"]]
    # change_df20["net_change"] = change_df20["net_change"] * 100
    return change_df


def backtest_analysis_result(stock, old_date):
    strat_count = buy_df.loc[buy_df["stockname"] == stock].shape[0]
    current_smas = str(list(buy_df.loc[buy_df["stockname"] == stock]["smas"]))
    temps = alltime_buy.loc[alltime_buy["stockname"] == stock]
    stockdf = pd.DataFrame()
    for i in list(buy_df.loc[buy_df["stockname"] == stock]["hpt_list"]):
        mini = temps.loc[temps["hpt_list"] == i]
        stockdf = pd.concat([stockdf, mini])
    strat_result_count = str(dict(stockdf["result"].value_counts()))
    strat_result_mean = stockdf["exit_result"].mean()
    stock_industry = list(buy_df.loc[buy_df["stockname"] == stock]["Industry"])[0]
    strat_test = temps.loc[temps["hpt_list"].isin(list(st_1["hpt_list"]))]
    stock_result_count = str(dict(strat_test["result"].value_counts()))
    stock_result_mean = strat_test["exit_result"].mean()
    investment_df = buy_df.loc[
        (buy_df["stockname"] == stock) & (buy_df["date"] == buy_df["date"].max())
    ]

    try:
        entry = list(investment_df["stock_entry"])[0]
        stoploss = list(investment_df["stop_loss"])[0]
        rupee_stop = list(investment_df["rupee_stop"])[0]
        quantity = list(investment_df["quantity"])[0]
        buy_amount = list(investment_df["total_buy_amount"])[0]
        investment_list = [entry, stoploss, rupee_stop, quantity, buy_amount]

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
        "stockname": stock,
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


def add_hpt(temp):
    temp["hpt_sma"] = "empty"
    temp.reset_index(drop=True, inplace=True)
    for index, row in temp.iterrows():
        hpt = []
        hpt.append(row["sma"])
        hpt.append(row["ema"])
        temp.loc[index, "hpt_sma"] = str(hpt)
    return temp


buy_df["smas"] = buy_df["hpt_list"].apply(sma_builder)
buy_df = buy_df.sort_values(["stockname", "smas"])
buy_df = buy_df.groupby(["stockname", "smas"]).first().reset_index()
buy_df = buy_df.sort_values(["stockname", "date"], ascending=False)

stock_df = pd.read_csv("./sectordf.csv", index_col=0)
stock_df.rename(columns={"stock": "stockname"}, inplace=True)
buy_df = buy_df.merge(stock_df, on="stockname", how="inner")

recent_stocks = list(set(list(buy_df.loc[buy_df["date"] >= today]["stockname"])))
change_df = get_net_change(main_df)

alltime_buy = pd.read_csv("./main_buyit_jan_2024.csv", index_col=0)
st_1 = pd.read_csv("./final_stratergy_jan_2024.csv", index_col=0)
st_1 = st_1.rename(columns={"new_hpt_lis": "hpt_list"})
buy_df["date"] = pd.to_datetime(buy_df["date"])
hptss_list = []
for i in st_1["hpt_list"]:
    hpt = eval(i)
    hpt.pop()
    hptss_list.append(str(hpt))
st_1["hpt_list"] = hptss_list

god_df = pd.read_csv("./paisabnaya_results_jan_2024.csv", index_col=0)
god_df = add_hpt(god_df)
god_df = god_df.loc[god_df["year"] != "2023-08-01"]

final_decision_df = pd.DataFrame()
for name in list(buy_df["stockname"].unique()):
    temp_df = backtest_analysis_result(name, "2023-11-24")
    rat_list = []
    for i in eval(list(temp_df["current_smas"])[0]):
        pl_df = god_df.loc[god_df["hpt_sma"] == i]
        pl_rat = pl_df["pl_ratio"].mean()
        pl_rat = round(pl_rat, 3)
        rat_list.append(pl_rat)
    rat_list = str(rat_list)
    temp_df["pl_ratio_list"] = rat_list
    final_decision_df = pd.concat([final_decision_df, temp_df])
    final_decision_df = final_decision_df[
        [
            "stockname",
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

recent_stocks = list(set(list(buy_df.loc[buy_df["date"] >= today]["stockname"])))
show_df = final_decision_df.loc[
    final_decision_df["stockname"].isin(recent_stocks)
].sort_values("strat_count", ascending=False)
show_df = show_df.reset_index()
fun_df = pd.read_csv("./fundamentals_res.csv", index_col=0)
show_df = show_df.merge(fun_df, on="stockname")
show_df = show_df.drop(columns=["current_smas", "pl_ratio_list"])
today_date = str(dt.date.today())
show_df["run_date"] = today_date
show_df.to_csv("./final_crossover_df.csv")
df_styled = show_df.style.background_gradient()
dfi.export(df_styled, "./crossover_table.png")
