#
# License: See LICENSE.md file
# GitHub: https://github.com/daivikbhatia/PySwing
#
import yfinance as yf
import pandas as pd
import os
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)


class StockAnalyzer:
    """
    A class used to analyze various financial indicators of a given stock.

    Attributes:
    ----------
    stock_name : str
        The stock ticker symbol.
    stock : Ticker
        A Yahoo Finance Ticker object for fetching stock data.
    indicator_count : int
        The count of indicators analyzed.
    total_score : int
        The total score based on the stock's financial indicators.
    scores : dict
        A dictionary containing the financial indicators and their values.
    financials : DataFrame
        The financial data for the stock, fetched from Yahoo Finance.
    """

    def __init__(self, stock_name):
        """
        Initializes the StockAnalyzer with the stock name and its financial data.

        Parameters:
        ----------
        stock_name : str
            The stock ticker symbol (e.g., 'AAPL', 'GOOG').
        """
        self.stock_name = stock_name
        self.stock = yf.Ticker(stock_name)
        self.indicator_count = 0
        self.total_score = 0
        self.scores = {}
        self.financials = self.stock.financials

    def calculate_profit_after_tax_growth(self, period="1y"):
        """
        Calculates the growth in profit after tax over a specified period.

        Parameters:
        ----------
        period : str, optional
            The period for calculating growth ('1y' or '2y'), default is '1y'.

        Returns:
        -------
        float
            The percentage growth in profit after tax, or 0 if an error occurs.
        """
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
        """
        Calculates the growth in total income (revenue) for the stock.

        Returns:
        -------
        float
            The percentage growth in income, or 0 if an error occurs.
        """
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
        """
        Assigns a score based on how a value fits within the given bounds.

        Parameters:
        ----------
        value : float
            The value to be scored.
        lower_bound : float
            The lower bound for the scoring range.
        upper_bound : float
            The upper bound for the scoring range.

        Returns:
        -------
        int
            The score (between 2 and 10) based on how well the value fits the bounds.
        """
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
        """
        Analyzes various financial indicators (like PE, PB, ROE) for the stock and calculates scores.
        """
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
        """
        Compiles the results of the analysis into a dictionary.

        Returns:
        -------
        dict
            A dictionary containing the stock's score, PAT growth, income growth, and other indicators.
        """
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
    """
    A class used to aggregate stock data for multiple stocks and save the analysis results.

    Attributes:
    ----------
    nse_file : str
        The file path for the NSE stock list.
    nse_list_final : list
        A list of NSE stock symbols with ".NS" suffix for Yahoo Finance API.
    results_df : DataFrame
        A DataFrame to store the analysis results for multiple stocks.
    """

    def __init__(self, nse_file):
        """
        Initializes the StockDataAggregator with the path to the NSE stock list.

        Parameters:
        ----------
        nse_file : str
            The file path of the CSV containing the NSE stock list.
        """
        self.nse_file = nse_file
        self.nse_list_final = self.prepare_nse_list()
        self.results_df = pd.DataFrame()

    def prepare_nse_list(self):
        """
        Prepares a list of NSE stock symbols with the ".NS" suffix for analysis.

        Returns:
        -------
        list
            A list of stock symbols.
        """
        nse = pd.read_csv(self.nse_file)
        nse_list = list(nse["Symbol"])
        return [f"{symbol}.NS" for symbol in nse_list]

    def analyze_stocks(self):
        """
        Analyzes all the stocks in the NSE list and stores the results in a DataFrame.
        """
        for stock_name in self.nse_list_final:
            try:
                analyzer = StockAnalyzer(stock_name)
                analyzer.analyze_indicators()
                results = analyzer.get_results()
                self.results_df = pd.concat([self.results_df, pd.DataFrame([results])])
            except Exception as e:
                print(f"Failed to analyze {stock_name}: {e}")

    def save_results(self, output_file):
        """
        Saves the analysis results to a CSV file.

        Parameters:
        ----------
        output_file : str
            The file path where the results will be saved.
        """
        self.results_df.reset_index(drop=True, inplace=True)
        self.results_df.to_csv(output_file, index=False)


if __name__ == "__main__":
    aggregator = StockDataAggregator(nse_file="../data/input/ind_nifty500list.csv")
    aggregator.analyze_stocks()
    aggregator.save_results("../data/processed/fundamentals_res.csv")

