# -*- coding: utf-8 -*-
"""
Created on Mon Mar 10 13:54:55 2025

@author: Asus
"""


import pandas as pd
import numpy as np
import argparse
import requests
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def fetch_omxh25_components():
    """Fetch OMXH25 Index Components from the API."""
    url = "https://api.nasdaq.com/api/nordic/instruments/FI0008900212/index-children?&lang=en"
    headers = {'User-Agent': 'Python/requests', 'Accept': 'application/json'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        return [item['isin'] for item in data['data']['instrumentListing']['rows']]
    except requests.RequestException as e:
        logging.error(f"Failed to fetch OMXH25 data: {e}")
        return []


def load_trade_data(file_path):
    """Load trade data from a CSV file."""
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        exit(1)

    try:
        df = pd.read_csv(file_path, parse_dates=["trade_timestamp"])
        df['price'] = abs(df['price'])  # Fix negative values
        df['volume'] = abs(df['volume'])
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        exit(1)


def profit_calculator(df, isin, rate, start_date, end_date = None):
    """"
    Calculates the profit over a certain period. 
    If end date is not indicated, calculates the profit for all the given data since the start day"""
    
    if end_date:
        trading = df.loc[(df.trade_timestamp.dt.strftime('%Y-%m-%d') >= start_date)&
                 (df.trade_timestamp.dt.strftime('%Y-%m-%d') <= end_date)&
                 (df["isin"] == isin)].copy()
    else: 
        trading = df.loc[(df.trade_timestamp.dt.strftime('%Y-%m-%d') >= start_date)&
                 (df["isin"] == isin)].copy()
    
    if trading.empty:
        logging.warning(f"No trade data found for this period, ISIN: {isin}")
        return 0
    
    trading['movement'] = None
    trading['profit'] = None
    

    for i in range(trading.shape[0]):

        # Add a new trade
        new_trade = np.array([[trading['volume'].iloc[i] if trading.side.iloc[i] == 'BUY' 
                                else -trading['volume'].iloc[i], 
                                trading['price'].iloc[i] 
                                ]])

        # Create the first row
        if i==0:
            trading.at[trading.index[i], 'movement'] = new_trade
            continue

        # Some values are missing
        if trading.side.iloc[i] is np.nan:
            trading.at[trading.index[i], 'movement'] = trading['movement'].iloc[i-1]  #Use the previous row if the data is missing
            logging.warning(f"Missing value for trade_id={trading.trade_id.iloc[i]}, price={trading.price.iloc[i]}, volume={trading.volume.iloc[i]}")
            continue

        # If two sequential operations are of the same sign (e.g., buy after buy), we do not register any profit/loss, so just stack those operations
        if np.sign(new_trade[0,0]) == np.sign(trading['movement'].iloc[i-1][0,0]):
            trading.at[trading.index[i], 'movement'] = np.vstack((trading['movement'].iloc[i-1], new_trade))

        # Here we want to distinguish short and long operations
        else:
            count = 0
            sign = np.sign(new_trade[0,0])

            # In this loop we check whether the new trade "disintegrates" the previous trades
            while new_trade[0,0]!=0 and trading['movement'].iloc[i-1][count:, 0].size>0:

                if sign == 1: new_trade[0,0] -= min(new_trade[0,0], abs(trading['movement'].iloc[i-1][count,0])) #previous deal was sell, now we buy
                if sign == -1: new_trade[0,0] += min(abs(new_trade[0,0]), trading['movement'].iloc[i-1][count,0])
                count+=1

            #logging.info(f"i={i}, count={count}")

            # Case when the new trade is completely offset by previous deals
            if new_trade[0,0]==0:

                trading.at[trading.index[i], 'movement'] = trading['movement'].iloc[i-1][count-1:].copy()  # Delete the first entries - those completely wiped out by the latest trade
                price = trading['movement'].iloc[i][0,1]
                balance = trading['movement'].iloc[i-1][:count, 0].sum() + trading.volume.iloc[i]*sign  # Calculate how much stocks are left in the first bunch that was not complitely eaten by the most recent deal
                trading.at[trading.index[i], 'movement'][0,:] = np.array([balance, price])

                cost = trading.movement.iloc[i-1][:count-1, 0] @ trading.movement.iloc[i-1][:count-1, 1] + (trading.movement.iloc[i-1][count-1, 0]-balance)*trading.movement.iloc[i][0, 1] # How much was spent on the shares sold/bought at the most recent trade

                revenue = (trading.volume.iloc[i] * trading.price.iloc[i])*-sign  #Since in this cycle we deal with the trades completely offset by previous deals, we are safe to calculate revenue like this. Note that we want to distinguish short and long cases
                trading.at[trading.index[i], 'profit'] =  revenue - cost 

                # debugging
                #logging.info(f"case 1, i={trading.index[i]}, сost={trading.movement.iloc[i-1][:count-1, 0], trading.movement.iloc[i-1][:count-1, 1], trading.movement.iloc[i-1][count-1, 0] - balance, trading.movement.iloc[i][0, 1]}")
                #logging.info(f'revenue = {trading.volume.iloc[i], trading.price.iloc[i]}')

            # Case when the most recent deal exceeds all the previous ones
            else: 

                trading.at[trading.index[i], 'movement'] = trading['movement'].iloc[i-1][count:].copy()
                price = trading.price.iloc[i]
                balance = new_trade[0,0]
                trading.at[trading.index[i], 'movement'] = np.array([[balance, price]])

                cost = trading.movement.iloc[i-1][:, 0] @ trading.movement.iloc[i-1][:, 1]
                revenue = trading.movement.iloc[i-1][0,0] * trading.price.iloc[i]
                trading.at[trading.index[i], 'profit'] =  revenue - cost

                #logging.info(f"case 2, i={trading.index[i]}, сost={trading.movement.iloc[i-1][:, 0], trading.movement.iloc[i-1][:, 1]}")   
                #logging.info(f'revenue = {trading.movement.iloc[i-1][0,0], trading.price.iloc[i]}')

        profit = trading.profit.sum()
        after_tax_profit = profit * (1-rate) if profit>0 else 0
        
        return after_tax_profit

'''
def save_results(results, date):
    """
    Save results to a CSV file
    """
    if results:
        df = pd.DataFrame([results])
        df.to_csv(f"{date}_tax.csv", index=False)
        logging.info(f"Saved tax calculation results to {date}_tax.csv")
'''

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, default = None)
    parser.add_argument('--file', type=str, default=r"fake_trades_limited_days.csv") 
    args = parser.parse_args()

    # Fetch index components
    index_components = fetch_omxh25_components()
    if not index_components:
        logging.error("No index components fetched. Exiting.")
        exit(1)

    # Load trade data
    df = load_trade_data(args.file)

    # Calculate profits
    results = 0
    for isin in df['isin'].unique():
        rate = 0.01 if isin in index_components else 0.15
        profit = profit_calculator(df, isin, rate, args.start_date, args.end_date)
        logging.info(f"Net income per ISIN={isin}: {profit:.2f}")
        results += profit
        
        
    
    if args.end_date:
        print(f'Total profit for the period from {args.start_date} to {args.end_date}: {results: .2f}')
    else:
        print(f'Total profit on {args.start_date}: {results: .2f}')
        
    # Save results
    #save_results(results, args.date)

    # Save OMXH25 components
    #pd.Series(index_components).to_csv(f"{args.date}_components.csv", header=False)


if __name__ == "__main__":
    main()




