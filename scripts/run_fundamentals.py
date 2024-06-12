import yfinance as yf
import pandas as pd


def calculate_stock_score(stock_name):
    def calculate_profit_after_tax_growth(ticker, period="1y"):
        # Fetching income statement data
        income_statement = yf.Ticker(ticker).financials.loc["Net Income"]

        # Selecting data for the specified period
        income_statement_period = income_statement.tail(2 if period == "1y" else 4)

        # Extracting profit after tax for the current and previous periods
        current_profit = income_statement_period.iloc[0]
        previous_profit = income_statement_period.iloc[-1]

        # Calculate profit after tax growth percentage
        profit_growth_percentage = (
            (current_profit - previous_profit) / abs(previous_profit)
        ) * 100

        return profit_growth_percentage

    def calculate_income_growth(ticker):
        # Fetching income statement data
        income_statement = yf.Ticker(ticker).financials.loc["Total Revenue"]

        # Calculate growth percentage
        current_income = income_statement.iloc[0]
        previous_income = income_statement.iloc[-1]
        income_growth_percentage = (
            (current_income - previous_income) / previous_income
        ) * 100

        return income_growth_percentage

    def calculate_score(value, lower_bound, upper_bound):
        if lower_bound <= value <= upper_bound:
            return 10
        elif (
            lower_bound * 0.8 <= value <= upper_bound * 1.2
        ):  # Tolerating slight deviation
            return 8
        elif (
            lower_bound * 0.6 <= value <= upper_bound * 1.4
        ):  # Tolerating moderate deviation
            return 6
        elif (
            lower_bound * 0.4 <= value <= upper_bound * 1.6
        ):  # Tolerating significant deviation
            return 4
        else:
            return 2

    # Create a Ticker object
    stock = yf.Ticker(stock_name)

    # Initialize scores and count of available indicators
    total_score = 0
    indicator_count = 0

    # Get financial metrics
    try:
        pe_ratio = stock.info["forwardPE"]  # Forward PE ratio
        pe_range = (10, 20)
        pe_score = calculate_score(pe_ratio, *pe_range)
        total_score += pe_score
        indicator_count += 1
    except KeyError:
        pe_ratio = 0

    try:
        pb_ratio = stock.info["priceToBook"]  # PB ratio
        pb_range = (1, 3)
        pb_score = calculate_score(pb_ratio, *pb_range)
        total_score += pb_score
        indicator_count += 1
    except KeyError:
        pb_ratio = 0

    try:
        roe = (
            stock.info["returnOnEquity"] * 100
        )  # ROE (multiply by 100 to get percentage)
        roe_range = (10, 20)
        roe_score = calculate_score(roe, *roe_range)
        total_score += roe_score
        indicator_count += 1
    except KeyError:
        roe = 0

    try:
        beta = stock.info["beta"]  # Beta
        beta_range = (0.8, 1.2)
        beta_score = calculate_score(beta, *beta_range)
        total_score += beta_score
        indicator_count += 1
    except KeyError:
        beta = 0

    try:
        dividend_yield = (
            stock.info["dividendYield"] * 100
        )  # Dividend yield (multiply by 100 to get percentage)
        dividend_yield_range = (1, 3)
        dividend_yield_score = calculate_score(dividend_yield, *dividend_yield_range)
        total_score += dividend_yield_score
        indicator_count += 1
    except KeyError:
        dividend_yield = 0

    try:
        debt_to_equity = stock.info["debtToEquity"]  # Debt to Equity ratio
        debt_to_equity_range = (0.5, 1)
        debt_to_equity_score = calculate_score(debt_to_equity, *debt_to_equity_range)
        total_score += debt_to_equity_score
        indicator_count += 1
    except KeyError:
        debt_to_equity = 0

    # Calculate final score (average of available scores)
    if indicator_count > 0:
        final_score = total_score / indicator_count
        print(
            f"Final Score for {stock_name}: {final_score:.2f} out of 10 for {indicator_count} indicators"
        )
    else:
        print("No indicators available for the stock.")

    try:
        pat = calculate_profit_after_tax_growth(stock_name)
        ti = calculate_income_growth(stock_name)
    except:
        pat = 0
        ti = 0

    df = pd.DataFrame()
    df["stockname"] = [stock_name]
    df["score"] = [final_score]
    df["total_indicators"] = [indicator_count]
    df["PE"] = [pe_ratio]
    df["PB"] = [pb_ratio]
    df["ROE"] = [roe]
    df["beta"] = [beta]
    df["dividend"] = [dividend_yield]
    df["DE"] = [debt_to_equity]
    df["pat%"] = [pat]
    df["ti%"] = [ti]
    return df


nse = pd.read_csv("./ind_nifty500list.csv")
nse_list = list(nse["Symbol"])
start_date = "2023-09-05"
nse_list_final = []

for i in nse_list:
    i = str(i)
    j = i + ".NS"
    nse_list_final.append(j)


fun_df = pd.DataFrame()
for stock in nse_list_final:
    try:
        df = calculate_stock_score(stock)
        fun_df = pd.concat([fun_df, df])
    except:
        pass
fun_df.reset_index(inplace=True, drop=True)
fun_df.to_csv("fundamentals_res.csv")
