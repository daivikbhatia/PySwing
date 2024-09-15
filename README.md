# PySwing

## Table of Contents
- [Swing\_trading](#PySwing)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Installation](#installation)
  - [Usage](#usage)
  - [License](#license)
  - [Acknowledgements](#acknowledgements)
  - [Word from me](#word-from-me)



## Introduction

**This project is intended for analysis purposes only and should not be considered as trading advice!**

This Repository uses simple moving average (SMA) crossovers to select stocks in Nifty500. The SMAs are selected after backtesting on last 3 years of data. We use yfinance to download the data.

Why SMA crossover?

We tested different technical indicators like RSI, Support resistance, etc. SMA crossover gave the best results after backtesting.

The SMA results are posted daily on my discord group mentioned below:
https://discord.gg/ZWKJ6FXM

## Features

Below is an example of the result that is published to discord daily

![alt text](crossover_table.png)

1) **stockName:** The name of the stock.
2) **stock industry:** Industry of that stock.
3) **net_industry_change:** Percentage change in that industry(nifty 500 stocks only) in last 20 days
4) **strat_count:** number of different SMA crossovers on which stock is listed today.
5) **strat_res_count_stock:** Stock performance results for last 1 year on current start_count SMA strategies.
6) **stat_res_mean_stock** Average profit made on start_count SMA strategies.
7) **stock_res_count_overall:**  Stock performance results for last 1 year on all SMA strategies.
8) **stock_res_mean_overall:** Average profit made on all SMA strategies.
9) **investment_data:** An array that represents [entry, stop_loss, gap_between_entry_&_SL,buy_quantity, exit]
10) **score:** Overall average fundamental score of the stock (out of 10) based on the below fundamentals.
11) **total_indecators:** Total number of fundamental indicators available for the stock (out of 8).
12) **PE:** PE ratio of the stock.
13) **PB:** PB ratio of the stock.
14) **ROE:** ROE (Return on Equity) of the stock.
15) **beta:** Beta value of the stock.
16) **dividend:** Dividend of the stock.
17) **DE:** Debt to Equity ratio of the stock.
18) **pat%:** PAT percentage growth of the stock.
19) **ti%:** total income growth of the stock.
20) **run_date:** Pipeline run date.

## Installation
Install ta-lib as a prerequisite.\
For example for macos:
```
brew install ta-lib
```
Follow below steps for installation. We recommend using Anaconda.

```
    git clone: https://github.com/daivikbhatia/PySwing.git
    cd PySwing

    conda create -n trading_env python=3.9
    conda activate trading_env
    pip install -r requirements.txt

```


## Usage
Please note that the below script will try to download a large input csv file. This file contains all the backtesting results.\
Run the below script to run entire pipeline:
```
cd scripts
./main_run.sh
```

## License

We use GNU GENERAL PUBLIC LICENSE

[Link to License](LICENSE)

## Acknowledgements

1) **Yfinance library:** Thankyou for providing free realtime stock data for almost all stocks out there!
2) Thanks to late night boredom and financial independency dream to kickstart this project.

## Word from me

I started this project out of a passion for coding, data science, and trading. My vision for this project is to continue expanding it with more functionality, features, and use cases.

Happy coding!
