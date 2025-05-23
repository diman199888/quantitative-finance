**Work in Progress**

This project analyzes the persistence of arbitrage opportunities between two cryptocurrency exchanges: Bybit and HTX. The goal is to understand under what market conditions arbitrage windows emerge and how long they last.

# Project Structure
The analysis is divided into two main parts:

## 1. Data Collection & Preprocessing

- Live order book data is collected from both exchanges via WebSocket.

- Bid/ask prices and sizes at multiple levels are stored in a local (PostgreSQL) database.

- Features such as spreads, imbalances, VWAPs, and relative quote strengths are engineered.

## 2. Machine Learning Modeling

- Features are selected based on statistical significance and market intuition. 

- Different machine learning model to predict the persistence (duration) of arbitrage windows are selected and compared.

- Hyperparameters of ML models are tuned.

# The folder contains 2 files: 

- crypto_arb_utils.py: package with all the functions related to data mining and data processing

- masterfile.ipynb: Colab/notebook where ML methods are applied to the preprocessed data

# Technologies Used
- Python: Polars, Pandas, Scikit-learn, XGBoost, Tensorflow, Optuna, WebSocket APIs, etc.

- PostgreSQL for fast local data storage

- Jupyter/Colab for interactive development
