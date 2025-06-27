# SUMMARY

This project investigates the persistence and profitability of ultra‐short-term arbitrage opportunities between two cryptocurrency exchanges: Bybit and HTX. The objective is to identify those price gaps that remain open long enough (at least 500 ms) to place and fully execute both legs of a cross-exchange trade (buy on one venue, sell on the other) without slippage. Because false signals are costly — it is a common occurence that due to fast price or liquidity changes, only one leg of the trade is executed, thus accumulating inventories that are likely to be sold at unfavourable prices later (in this project, inventories are hold for 60 seconds) — our loss on a mistaken opportunity can be four to five times greater than the gain from a successful arbitrage. Hence, various ML models are applied to better separate two cases; however, at this point they do not show a significant improvement in profitability compared with the naive ("always‐trade") model. Backtesting indicates that $10.000 yields about $35 in net profit over a 12-hour trading session (1.300 trades).

#### Latest update:
- New approach to the classification of arbitrage opportunities: now factors in available liquidity and assumes open orders persist for up to 60s before execution or cancellation
- Dealing with class imbalance: unsuccessful attempt
- Introduced custom loss function to make hyperparameter tuning more relevant
- Realistic backtest framework: Added transaction fees, capital constraints, and daily inventory carry-forward (to account for idle time) to more accurately simulate PnL on held positions.
  
#### Next steps:
- New ML approaches to better separate two cases
- Real-life strategy implementation

# Project Structure
The analysis is divided into two main parts:

## 1. Data Collection & Preprocessing

- Live tick order book data is collected from both exchanges via WebSocket.

- Bid/ask prices and sizes at multiple levels are stored in a local (PostgreSQL) database.

- Features such as spreads, imbalances, and relative quotes, as well as their lags are engineered.

## 2. Machine Learning Modeling and Backtesting

- Different machine learning model to predict the persistence (duration) of arbitrage windows are selected and compared.

- Hyperparameters of ML models are tuned.

- Backtesting is implemented 

# The folder contains 2 files: 

- [crypto_arb_utils.py](./crypto_arb_utils.py): package with all the functions related to data mining and data processing

- [masterfile.ipynb](./masterfile.ipynb): Colab/notebook where ML methods are applied to the preprocessed data

- [backtest.log](./backtest.log): logger output of backtesting

# Technologies Used
- Python: Polars, Pandas, Scikit-learn (incl. cuML), Imblearn, XGBoost, Tensorflow, Optuna, WebSocket APIs, Logging etc.

- PostgreSQL for fast local data storage

- Jupyter/Colab for interactive development
