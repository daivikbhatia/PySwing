#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/swing_trading
#
import yfinance as yf
import pandas as pd
import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


class StockAnalyzer:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.stock = yf.Ticker(stock_name)
        self.indicator_count = 0
        self.total_score = 0
        self.scores = {}
        self.financials = self.stock.financials

    def calculate_profit_after_tax_growth(self, period="1y"):
        try:
            income_statement = self.financials.loc["Net Income"]
            income_statement_period = income_statement.tail(2 if period == "1y" else 4)
            current_profit = income_statement_period.iloc[0]
            previous_profit = income_statement_period.iloc[-1]
            profit_growth_percentage = (
                (current_profit - previous_profit) / abs(previous_profit)
            ) * 100
            return profit_growth_percentage
        except Exception:
            return 0

    def calculate_income_growth(self):
        try:
            income_statement = self.financials.loc["Total Revenue"]
            current_income = income_statement.iloc[0]
            previous_income = income_statement.iloc[-1]
            income_growth_percentage = (
                (current_income - previous_income) / previous_income
            ) * 100
            return income_growth_percentage
        except Exception:
            return 0

    def calculate_score(self, value, lower_bound, upper_bound):
        if lower_bound <= value <= upper_bound:
            return 10
        elif lower_bound * 0.8 <= value <= upper_bound * 1.2:
            return 8
        elif lower_bound * 0.6 <= value <= upper_bound * 1.4:
            return 6
        elif lower_bound * 0.4 <= value <= upper_bound * 1.6:
            return 4
        else:
            return 2

    def analyze_indicators(self):
        indicators = {
            "PE": ("forwardPE", (10, 20)),
            "PB": ("priceToBook", (1, 3)),
            "ROE": ("returnOnEquity", (10, 20), True),
            "Beta": ("beta", (0.8, 1.2)),
            "Dividend Yield": ("dividendYield", (1, 3), True),
            "Debt to Equity": ("debtToEquity", (0.5, 1)),
        }

        for name, (key, bounds, *is_percentage) in indicators.items():
            try:
                value = self.stock.info[key]
                if is_percentage:
                    value *= 100
                score = self.calculate_score(value, *bounds)
                self.total_score += score
                self.indicator_count += 1
                self.scores[name] = value
            except KeyError:
                self.scores[name] = None

    def get_results(self):
        if self.indicator_count > 0:
            final_score = self.total_score / self.indicator_count
        else:
            final_score = 0

        pat_growth = self.calculate_profit_after_tax_growth()
        income_growth = self.calculate_income_growth()

        results = {
            "Stock Name": self.stock_name,
            "Score": final_score,
            "Total Indicators": self.indicator_count,
            "PAT Growth (%)": pat_growth,
            "Income Growth (%)": income_growth,
        }
        results.update(self.scores)
        return results


class StockDataAggregator:
    def __init__(self, nse_file):
        self.nse_file = nse_file
        self.nse_list_final = self.prepare_nse_list()
        self.results_df = pd.DataFrame()

    def prepare_nse_list(self):
        nse = pd.read_csv(self.nse_file)
        nse_list = list(nse["Symbol"])
        return [f"{symbol}.NS" for symbol in nse_list]

    def analyze_stocks(self):
        for stock_name in self.nse_list_final:
            try:
                analyzer = StockAnalyzer(stock_name)
                analyzer.analyze_indicators()
                results = analyzer.get_results()
                self.results_df = pd.concat([self.results_df, pd.DataFrame([results])])
            except Exception as e:
                print(f"Failed to analyze {stock_name}: {e}")

    def save_results(self, output_file):
        self.results_df.reset_index(drop=True, inplace=True)
        self.results_df.to_csv(output_file, index=False)


if __name__ == "__main__":
    aggregator = StockDataAggregator(nse_file="../data/input/ind_nifty500list.csv")
    aggregator.analyze_stocks()
    aggregator.save_results("../data/processed/fundamentals_res.csv")
