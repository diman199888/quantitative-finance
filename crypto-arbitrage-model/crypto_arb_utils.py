# -*- coding: utf-8 -*-
"""
Created on Tue Apr 22 12:39:52 2025

@author: Asus
"""

import pandas as pd
import websockets
import time
import json
import gzip
import nest_asyncio
import asyncio
import logging
import polars as pl
import psycopg2
import numpy as np
from functools import reduce

###############################################################################
################## PART I: GET DATA ########################################### 
###############################################################################

# Set up logging module
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


nest_asyncio.apply()


#Support functions:
def refresh(snapshot, exchange): 
    '''
    Function to download snapshots
    '''
    ts = snapshot["ts"]
    if exchange == "HTX":
        bids = snapshot["tick"]["bids"]
        asks =  snapshot["tick"]["asks"]
    elif exchange == "Bybit":
        bids = snapshot["data"]["b"]
        bids = [[float(price), float(size)] for price, size in bids]
        asks =  snapshot["data"]["a"]
        asks = [[float(price), float(size)] for price, size in asks]
    return ts, bids, asks
    

def HTX_update(delta, bids, asks): 
    '''
    Function to update HTX's snapshots
    '''
    ts = delta['ts']
        
    if 'asks' in delta['tick']: 
        asks = dict(asks)
        update = delta['tick']['asks']
        
        for price, size in update:
            if size == 0:
                asks.pop(price, None)
            else:
                asks[price] = size
                
        asks = [list(i) for i in sorted(asks.items())]
    
    if 'bids' in delta['tick']: 
        bids = dict(bids)
        update = delta['tick']['bids']

        for price, size in update:
            if size == 0:
                bids.pop(price, None)
            else:
                bids[price] = size
                
        bids = [list(i) for i in sorted(bids.items(), reverse=True)]
            
    return ts, bids, asks

def Bybit_update(delta, bids, asks): 
    '''
    Function to update Bybit's snapshots
    '''
    ts = delta['ts']
        
    if delta['data']['a']: 
        asks = dict(asks)
        update = delta['data']['a']
        update = [[float(price), float(size)] for price, size in update]
        
        for price, size in update:
            if size == 0:
                asks.pop(price, None)
            else:
                asks[price] = size
                
        asks = [list(i) for i in sorted(asks.items())]
    
    if delta['data']['b']: 
        bids = dict(bids)
        update = delta['data']['b']
        update = [[float(price), float(size)] for price, size in update]

        for price, size in update:
            if size == 0:
                bids.pop(price, None)
            else:
                bids[price] = size
                
        bids = [list(i) for i in sorted(bids.items(), reverse=True)]
            
    return ts, bids, asks

def store_data(exchange, ts, bids, asks):
    '''
    Function to store data in SQL
    '''
    global last_market_data
    if not last_market_data[exchange]:
        cursor.execute("INSERT INTO data (exchange, ts, side, level1, level2, level3) VALUES (%s, %s, %s, %s, %s, %s)", (exchange, ts, "bid", json.dumps(bids[0]), json.dumps(bids[1]), json.dumps(bids[2])))
        cursor.execute("INSERT INTO data (exchange, ts, side, level1, level2, level3) VALUES (%s, %s, %s, %s, %s, %s)", (exchange, ts,"ask", json.dumps(asks[0]), json.dumps(asks[1]), json.dumps(asks[2])))
        connection.commit()
        
    else: 
        if last_market_data[exchange]['bids'][:3] != bids[:3]:
            cursor.execute("INSERT INTO data (exchange, ts, side, level1, level2, level3) VALUES (%s, %s, %s, %s, %s, %s)", (exchange, ts, "bid", json.dumps(bids[0]), json.dumps(bids[1]), json.dumps(bids[2])))
            connection.commit()
        if last_market_data[exchange]['asks'][:3] != asks[:3]:
            cursor.execute("INSERT INTO data (exchange, ts, side, level1, level2, level3) VALUES (%s, %s, %s, %s, %s, %s)", (exchange, ts,"ask", json.dumps(asks[0]), json.dumps(asks[1]), json.dumps(asks[2])))
            connection.commit()

async def get_htx():
    
    """
    Function to upload bids/asks from HTX; more info on https://huobiapi.github.io/docs/spot/v1/en
    """
    
    uri = "wss://api-aws.huobi.pro/feed"
    
    refresh_topic = "market.ltcusdt.mbp.refresh.5"
    delta_topic = "market.ltcusdt.mbp.5"

    refresh_sub = {"sub": refresh_topic, "id": "htx_refresh"}
    delta_sub = {"sub": delta_topic, "id": "htx_delta"}

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps(refresh_sub))
                await websocket.send(json.dumps(delta_sub))
                logger.info('received data from HTX')

                while True:
                    compressed = await websocket.recv()
                    data = json.loads(gzip.decompress(compressed).decode())
                    #logging.info(f'{data}')
                    
                    if "ping" in data:
                        await websocket.send(json.dumps({"pong": data["ping"]}))
                        continue

                    # Handle refresh updates
                    if data.get("ch") == refresh_topic:
                        
                        #logging.info(f"HTX snapshot: {data}")
                        htx_ts, htx_bids, htx_asks = refresh(data, 'HTX')   
                        store_data("HTX", htx_ts, htx_bids, htx_asks)
                        last_market_data["HTX"] = {'ts': htx_ts, "bids": htx_bids, "asks": htx_asks}
                        
                    # Handle delta updates
                    elif data.get("ch") == delta_topic:
                        
                        #logging.info(f"HTX delta: {data}")
                        htx_ts, htx_bids, htx_asks = HTX_update(data, htx_bids, htx_asks)
                        store_data("HTX", htx_ts, htx_bids, htx_asks)
                        last_market_data["HTX"] = {'ts': htx_ts, "bids": htx_bids, "asks": htx_asks}                    
                                         

        except Exception as e:
            logging.error(f"HTX Error: {e}")
            await asyncio.sleep(0.1)  # Add short pause before reconnecting
            
async def get_bybit():
    
    """
    Function to upload bids/asks from Bybit. More info on https://bybit-exchange.github.io/docs/v5
    """

    uri = "wss://stream.bybit.com/v5/public/spot"
    last_ping_time = time.time()
    
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                
                # Subscribe to Bybit stream 
                subscribe_msg = {"op": "subscribe", "args": ["orderbook.50.LTCUSDT"]}
                await websocket.send(json.dumps(subscribe_msg))
                logger.info('received data from Bybit')
                
                while True:
                    if time.time() - last_ping_time > 20:  #Bybit doesn't send pongs, but expects us to send ping every 20 second
                        await websocket.send(json.dumps({"op": "ping", "req_id": "keepalive"}))
                        last_ping_time = time.time()
                                            
                   
                    # Receive data; unlike HTX, Bybit sends decompressed data
                    response = await websocket.recv()
                    data = json.loads(response)

                    #logging.info(f"Bybit Raw Data: {data}")

                    # Handle refresh updates
                    if data.get("type") == 'snapshot':
                        
                        #logging.info(f"Bybit snapshot: {data}\n")
                        bybit_ts, bybit_bids, bybit_asks = refresh(data, 'Bybit')   
                        store_data("Bybit", bybit_ts, bybit_bids, bybit_asks)
                        last_market_data["Bybit"] = {'ts': bybit_ts, "bids": bybit_bids, "asks": bybit_asks}
                        
                    # Handle delta updates
                    elif data.get("type") == "delta":
                        
                        #logging.info(f"Bybit delta: {data}\n")
                        bybit_ts, bybit_bids, bybit_asks = Bybit_update(data, bybit_bids, bybit_asks)
                        store_data("Bybit", bybit_ts, bybit_bids, bybit_asks)
                        last_market_data["Bybit"] = {'ts': bybit_ts, "bids": bybit_bids, "asks": bybit_asks}                  
                                             
    
        except Exception as e:
            logging.error(f"Bybit Error: {e}")
            await asyncio.sleep(0.1)  # Add short pause before reconnecting
            

async def main():
    """We use async function to get data from two exchanges simultaneously"""
    try: 
        await asyncio.gather(get_htx(), get_bybit())
    except KeyboardInterrupt:
        print("\n### Interrupted ###")

if __name__ == "__main__":
    
    connection = psycopg2.connect(host = "localhost", 
                                  dbname = "postgres", 
                                  user = 'postgres', 
                                  password = '  ', 
                                  port = 5432)
    
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS data(exchange TEXT, ts BIGINT, side TEXT, \
                   level1 TEXT, level2 TEXT, level3 TEXT)")
    connection.commit
    
    last_market_data = {"HTX": None, "Bybit": None} ## store data here
    
    try:
        asyncio.run(main())
        logger.info('data collection started')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
    except KeyboardInterrupt:
        print("\n### Interrupted ###")
        
        
        
        
        
        
        

###############################################################################
################## PART II: MANIPULATE DATA ###################################
###############################################################################

def download_data(engine) -> pl.DataFrame: 
    """
    Read raw order‐book snapshots from a SQL table, unpack the JSON levels, and return
    a Polars DataFrame ready for downstream processing.

    This function will:
      1. Query the table named `"data"` via the given SQLAlchemy `engine`.  
      2. For each of the JSON‐encoded columns `"level1"`, `"level2"`, and `"level3"`:
         - Parse the JSON string into a Python dict/list.
         - Split each dict/list into two new columns:
           - `<level>_price` (e.g. `level1_price`)
           - `<level>_size`  (e.g. `level1_size`)
         - Drop the original JSON column.
      3. Convert the resulting pandas.DataFrame into a Polars DataFrame for fast, memory‐efficient analytics.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        An open SQLAlchemy engine or connection pointing at a database that contains
        a table called `"data"`.  That table must have at least the following columns:
          - `exchange` (TEXT)
          - `ts`       (INTEGER timestamp, e.g. epoch ms)
          - `side`     (TEXT, e.g. "bid" or "ask")
          - `level1`, `level2`, `level3` (TEXT JSON-encoded arrays of `[price, size]`)

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame with:
          - All original non‐JSON columns (`exchange`, `ts`, `side`), plus
          - `level1_price`, `level1_size`,
          - `level2_price`, `level2_size`,
          - `level3_price`, `level3_size`
    """
    
    start_ts = time.time()
    
    try:
        logger.info("Connecting to database…")
        df = pl.read_database("SELECT * FROM data", connection = engine)
        logger.info("Data loaded")
        
        for lvl in ["level1", "level2", "level3"]:
            df = df.with_columns([
                # parse JSON into a List(UInt64) or List(Float64)
                pl.col(lvl)
                  .str.json_decode()
                  .list.get(0)                # first element
                  .alias(f"{lvl}_price"),
    
                pl.col(lvl)
                  .str.json_decode()
                  .list.get(1)
                  .alias(f"{lvl}_size"),
            ]).drop(lvl)                      # drop the original JSON column
            
    except Exception as e:
        logger.exception(f"Failed to load data: {e}")
        raise
    
    logger.info(f"Loaded rows:{len(df):,d}\nTime elapsed:{time.time() - start_ts:.2f} s")
    
    return df


def get_orderbook_snapshot(df: pl.DataFrame, exchange: str) -> pl.DataFrame:
    """
    Pivot raw order‐book updates into a snapshot of bid and ask levels per timestamp.

    Given a Polars DataFrame `df` containing tick‐level order‐book rows for a single
    exchange, this function reshapes it into one row per timestamp with separate
    columns for bids and asks at each depth level.


    The function will:
      1. Filter to only rows for the specified `exchange` (if your DataFrame
         contains multiple exchanges).
      2. Pivot the `side` column into two branches (`bid` vs. `ask`), aligning
         each level’s price and size side by side.
      3. Rename the pivoted columns to include both the exchange name and the side,
         e.g. `HTX_bid_price_level1`, `HTX_ask_size_level2`, etc.
      4. Return a time‐sorted Polars DataFrame with one row per unique `ts` and
         columns:
           - `ts`
           - `{exchange}_bid_levelN_price`, `{exchange}_bid_levelN_size`
           - `{exchange}_ask_levelN_price`, `{exchange}_ask_levelN_size`
           - `delta_ts`, the elapsed time in milliseconds since the previous record

    Parameters
    ----------
    df : pl.DataFrame
        Raw order‐book snapshot DataFrame in “long” form:
        multiple rows per `ts`, one for each side (`bid`/`ask`) and depth level.
    exchange : str
        The exchange name used as a prefix in the column names (e.g. `"HTX"` or `"Bybit"`).

    Returns
    -------
    pl.DataFrame
    """
    try:
        bid = df.filter((pl.col("exchange") == exchange) & (pl.col("side") == "bid")).drop(["exchange", 'side'])
        bid = bid.rename(lambda col: col if col == "ts" else f"{col}_bid")
    
        ask = df.filter((pl.col("exchange") == exchange) & (pl.col("side") == "ask")).drop(["exchange", 'side'])
        ask = ask.rename(lambda col: col if col == "ts" else f"{col}_ask")
    
        new_df = bid.join(ask, on = 'ts', how = 'full', coalesce = True)
        new_df = new_df.sort("ts")
        new_df = new_df.fill_null(strategy="forward")
        new_df = new_df.with_columns([(pl.col("ts") - pl.col("ts").shift(1)).alias("delta_ts")])
        
        logger.info(f"DataFrame for {exchange} processed. Number of rows:\n{len(new_df):,d}")
    
    except Exception as e:
        logger.exception(f"Failed to load data: {e}")
        raise
            
    return new_df


def merge_data(df1: pl.DataFrame, exchange1: str, df2: pl.DataFrame, exchange2: str) -> pl.DataFrame:
    """
    Time-align and merge two per-exchange order-book DataFrames into a single view.

    This function takes two Polars DataFrames—each containing tick-level snapshots
    for a different exchange—and produces one fully synchronized DataFrame:
    
      1. **Rename columns**  
         All non-`ts` columns in `df1` are prefixed with `exchange1_`, and likewise
         `df2`’s columns are prefixed with `exchange2_`.  
      2. **Full outer join on `ts`**  
         We join on the timestamp, ensuring that every tick from either exchange
         appears in the result.  Common columns are coalesced so that if both
         exchanges report the same field, the non-null value is kept.  
      3. **Sort by time**  
         The merged DataFrame is ordered chronologically by `ts`.  
      4. **Forward-fill missing values**  
         Any `null` in an exchange-specific column is replaced by the most recently
         seen value (so each column always reflects the latest known state).  
      5. **Drop residual null rows**  
         If there are still rows with missing critical fields after fill-forward,
         they are removed.

    Parameters
    ----------
    df1 : pl.DataFrame
        The first exchange’s DataFrame.  Must contain a `ts` column plus any other
        fields (prices, sizes, deltas, etc.) to be time-aligned.
    exchange1 : str
        The string prefix to apply to `df1`’s non-`ts` columns (e.g. `"HTX"`).
    df2 : pl.DataFrame
        The second exchange’s DataFrame, same requirements as `df1`.
    exchange2 : str
        The string prefix to apply to `df2`’s non-`ts` columns (e.g. `"Bybit"`).

    Returns
    -------
    pl.DataFrame
        A new DataFrame with:
          - One `ts` column (timestamps from either source)
          - All original fields from both inputs, renamed to `<exchange>_<field>`
          - Fully time-sorted and forward-filled so each column carries the latest
            known data at every timestamp.
          - timedelta column: measures how much time elapsed between 2 recordings from different exchanges
    """
    df1 = df1.rename(lambda col: col if col == "ts" else f"{exchange1}_{col}")
    df2 = df2.rename(lambda col: col if col == "ts" else f"{exchange2}_{col}")

    new_df = df1.join(df2, on="ts", how="full", coalesce=True)
    new_df = new_df.sort("ts")

    # Create timestamp markers
    ts1 = new_df.select([
        pl.when(pl.col(f"{exchange1}_level1_price_bid").is_not_null())
          .then(pl.col("ts"))
          .otherwise(None)
          .forward_fill()
    ])

    ts2 = new_df.select([
        pl.when(pl.col(f"{exchange2}_level1_price_bid").is_not_null())
          .then(pl.col("ts"))
          .otherwise(None)
          .forward_fill()
    ])

    timedelta = pl.Series("timedelta", (ts1 - ts2))
    
    new_df = new_df.with_columns([timedelta])
    new_df = new_df.with_columns([
        pl.when(pl.col(f"{exchange1}_delta_ts").is_null())
          .then(pl.col("timedelta").abs())
          .otherwise(pl.col(f"{exchange1}_delta_ts"))
          .alias(f"{exchange1}_delta_ts"),

        pl.when(pl.col(f"{exchange2}_delta_ts").is_null())
          .then(pl.col("timedelta").abs())
          .otherwise(pl.col(f"{exchange2}_delta_ts"))
          .alias(f"{exchange2}_delta_ts")
    ])
    
    new_df = new_df.fill_null(strategy="forward")
    new_df = new_df.drop_nulls()
    
    logger.info(f"DataFrames for {exchange1} and {exchange2} processed. \nNumber of rows: {len(new_df):,d}")
    
    return new_df


def strategy(df: pl.DataFrame, exchange1: str, exchange2: str) -> pl.DataFrame:
    """
    Compute the one‐tick arbitrage profit for buying on one exchange and selling on another.

    Given a merged order‐book DataFrame with top‐of‐book prices for two venues,
    this function adds a column that measures the potential return (in percent)
    if you were to buy at the best ask on `exchange1` and immediately sell at the best bid on `exchange2`.

    Parameters
    ----------
    df : pl.DataFrame
        A time‐sorted DataFrame containing at minimum the following columns:
          - `ts`: timestamp of each snapshot (e.g. in ms)
          - `{exchange1}_level1_price_ask`: best ask price on the buy side
          - `{exchange2}_level1_price_bid`: best bid price on the sell side
    exchange1 : str
        The name or prefix used for the “buy” exchange in your column names.
        e.g. "HTX" if your columns are `HTX_level1_price_ask`, etc.
    exchange2 : str
        The name or prefix used for the “sell” exchange in your column names.
        e.g. "Bybit" if your columns are `Bybit_level1_price_bid`, etc.

    Returns
    -------
    pl.DataFrame
        A copy of `df` with one additional column:
        `arb_opportunity` (float):
            - For each row,  
              `( {exchange2}_level1_price_bid / {exchange1}_level1_price_ask - 1 ) * 100`  
            - Positive values indicate a theoretical arbitrage profit (in percent).  
            - Values ≤ 0 are set to zero (no profitable opportunity).

    """
    strategy = df.select([
            # Arbitrage opportunity (e.g., Buy HTX, Sell Bybit)
            pl.when(pl.col(f"{exchange2}_level1_price_bid") / pl.col(f"{exchange1}_level1_price_ask") > 1)
            .then((pl.col(f"{exchange2}_level1_price_bid") / pl.col(f"{exchange1}_level1_price_ask") - 1) * 100)
            .otherwise(
                        pl.when(pl.col(f"{exchange1}_level1_price_bid") / pl.col(f"{exchange2}_level1_price_ask") > 1)
                        .then((pl.col(f"{exchange1}_level1_price_bid") / pl.col(f"{exchange2}_level1_price_ask") - 1) * -100)
                        .otherwise(0              
                        )
            )
            .alias("arb_opportunity")
    ])
    
    return_df = df.with_columns([pl.Series(strategy)])
    
    logger.info(f"DataFrame processed. Number of active arbitrage window rows\
        \nFor strategy 'buy at {exchange1}, sell at {exchange2}':\
        {return_df.filter(pl.col('arb_opportunity') > 0).shape[0]: ,d} \
        \nFor strategy 'buy at {exchange2}, sell at {exchange1}':\
        {return_df.filter(pl.col('arb_opportunity') < 0).shape[0]: ,d}"
    )
    
    return return_df


def duration_calculation(df: pl.DataFrame) -> pl.DataFrame:
    
    """
    Identify arbitrage windows in a tick-level time series and compute their start/end markers
    and durations.

    For each row in `df`, which must contain:
      - `ts`: a monotonically increasing timestamp (e.g. in milliseconds)
      - `arb_opportunity`: the current arbitrage spread

    this function will:

    1. **Flag window boundaries**  
       - `arbitrage_start` (bool): True on the first tick where `arb_opportunity` crosses ±0.03 from zero.  
       - `active_arbitrage` (bool): True on every tick while |`arb_opportunity`| ≥ 0.03.  
       - `discrepancy_start` (datetime): the timestamp of the first non-zero arbitrage tick, forward-filled  
         so every row in that window knows when it began.  
       - `arbitrage_end` (datetime): the timestamp of the first tick back inside (|`arb_opportunity`| < 0.03),  
         forward-filled so every row in the next idle period knows when the prior window closed.

    2. **Assign window IDs**  
       - `arbitrage_id` (int): a running counter that increases by one on each `arbitrage_start`,  
         but only during active windows.

    3. **Compute tick-to-tick intervals**  
       - `length` (int): the difference in `ts` to the *next* tick.  
       - Sum these `length` values per `arbitrage_id` to get the **total duration** of each window.

    4. **Merge durations back**  
       Joins the per-`arbitrage_id` summed durations into every row of that window.

    5. **Compute relative times**  
       - `till_arbitrage`: `ts – discrepancy_start`, the elapsed time since the window opened.  
       - `since_last_arbitrage`: `ts – arbitrage_end`, the elapsed time since the last window closed.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with all the original columns plus:
          - `arbitrage_start`, `active_arbitrage`, `discrepancy_start`, `arbitrage_end`
          - `arbitrage_id`
          - `length` (inter-tick delta), and summed window duration
          - `till_arbitrage`, `since_last_arbitrage`
    """

    df_n = df.with_columns([
        # Flag arbitrage start
        (
            (
                (pl.col("arb_opportunity") >= 0.03) & (pl.col("arb_opportunity").shift(1) < 0.03)
            ) | (
                (pl.col("arb_opportunity") <= -0.03) & (pl.col("arb_opportunity").shift(1) > -0.03)
            )
        ).alias("arbitrage_start"),
    
        # Flag active arbitrage rows
        ((pl.col("arb_opportunity") >= 0.03) | (pl.col("arb_opportunity") <= -0.03)).alias("active_arbitrage"),

        pl.when(
            (
                (pl.col("arb_opportunity") > 0.0) & (pl.col("arb_opportunity").shift(1) == 0.0)
            ) | (
                (pl.col("arb_opportunity") < -0.0) & (pl.col("arb_opportunity").shift(1) == 0.0)
            )
        )
        .then(pl.col('ts'))
        .otherwise(        
            pl.when(
                (
                    (pl.col("arb_opportunity") == 0.0) & (pl.col("arb_opportunity").shift(1) > 0.0)
                ) | (
                    (pl.col("arb_opportunity") == -0.0) & (pl.col("arb_opportunity").shift(1) < 0.0)
                    )
               )
                .then(pl.col('ts'))
            )
        .forward_fill()
        .alias("discrepancy_start"), 

        pl.when(
            (
                (pl.col("arb_opportunity") <= 0.03) & (pl.col("arb_opportunity").shift(1) >= 0.03)
            ) | (
                (pl.col("arb_opportunity") >= -0.03) & (pl.col("arb_opportunity").shift(1) <= -0.03)
            )
        ).then(pl.col('ts'))
        .forward_fill()
        .alias("arbitrage_end")
    ])
    
    # Assign arbitrage_id by cumulative sum of starts
    df_n = df_n.with_columns([
        pl.when(pl.col("active_arbitrage"))
        .then(pl.cum_sum("arbitrage_start"))
        .otherwise(None)
        .alias("arbitrage_id")        
    ])
    
    # Calculate tick-to-tick duration
    df_n = df_n.with_columns([
        (pl.col("ts").shift(-1) - pl.col("ts")).alias("length")
    ])
    
    # Group by arbitrage_id and sum durations
    durations = (
        df_n.drop_nulls("arbitrage_id")
        .group_by("arbitrage_id", maintain_order=True)
        .agg(pl.sum("length"))
        )

    
    df_n = df_n.drop("length")
    
    # Merge the duration info back into the original frame
    df_n = df_n.join(durations, on="arbitrage_id", how="left")

    
    df_n = df_n.with_columns([
        (pl.col("ts") - pl.col("discrepancy_start"))
        .alias("till_arbitrage"),
        (pl.col("ts") - pl.col("arbitrage_end"))
        .alias("since_last_arbitrage")
        ]).drop(['discrepancy_start', 'arbitrage_end'])
    
    logger.info(f"DataFrame processed. \
                \nNumber of arbitrages: {df_n['arbitrage_id'].max():,d}\
                ")
    
    return df_n


'''
def final_manipulations(df: pd.DataFrame, include_first_observation: bool = True, max_delay_sec: int = 60, n_lags: int = None, 
                        without_lags: list = ['ts','active_arbitrage','arbitrage_start','session_number','arbitrage_id','length','till_arbitrage','since_last_arbitrage','Bybit_delta_ts','HTX_delta_ts','timedelta', 'night_hours']) -> pd.DataFrame:
    
    """
    Perform the final round of cleaning and feature‐engineering on the merged arbitrage dataset.

    This function will:
      1. Optionally drop the very first arbitrage observation (since it is common to see arbitrages that last one tick).
      2. Remove any arbitrage events whose inter‐arrival delay exceeds `max_delay_sec`.
      3. Generate up to `n_lags` lagged versions of all numeric feature columns.
      4. Return a clean DataFrame ready for modelling.

    Parameters
    ----------
    df : pd.DataFrame
        The merged, time‐sorted data from both exchanges, including columns like
        “arbitrage_start”, per‐exchange `delta_ts`, and any other engineered features.
    include_first_observation : bool, default=True
        If False, drop the first arbitrage event tick that occurs.
        If True, keep those first‐after‐gap rows.
    max_delay_sec : int, default=60
        Any row whose delay since the previous arbitrage (in seconds) exceeds
        this threshold will be dropped. Helps remove spurious or stale events
        at session boundaries.
    n_lags : int or None, default=None
        If an integer, create lagged features up to `n_lags` periods for every
        numeric column (e.g. `feature`, `feature_lag_1`, …, `feature_lag_n_lags`).
        If None, no lagged features are added.
    without_lags : list, default = ['ts','active_arbitrage','arbitrage_start','session_number','arbitrage_id','length','till_arbitrage','since_last_arbitrage','Bybit_delta_ts','HTX_delta_ts','timedelta']
        List of variables, lags of which would not be added in the final model

    Returns
    -------
    pd.DataFrame
        A cleaned and fully feature‐engineered DataFrame, with:
          - unwanted session‐boundary rows removed,
          - excessive‐delay rows dropped,
          - optional lag‐features appended,
          - a fresh, zero‐based integer index.
    """
    
    # To start with, we need to exclude the very first and last arbitrage observation of each recording session, as it may bring bias
    df = df.copy()
    df['session_number'] = np.where(
                                        ((df.ts - df.ts.shift(1) > 5*60*1000) | (
                                            ((df.ts - df.ts.shift(1)).isna())
                                        )), 1, np.nan)
    df.session_number = df.session_number.cumsum().ffill()
    id_to_exclude = df.groupby(by = 'session_number')['arbitrage_id'].min().values
    
    id_to_exclude = np.append(id_to_exclude, id_to_exclude - 1)

    # We also want to get rid of the data that may be recorded with delays
    delays = df.groupby(by = 'arbitrage_id')[['Bybit_delta_ts', 'HTX_delta_ts', 'timedelta']].max()
    id_to_exclude = np.append(id_to_exclude,
        delays[
            (delays.Bybit_delta_ts > 1000*max_delay_sec) | (delays.HTX_delta_ts > 1000*max_delay_sec) | (delays.timedelta.abs() > 1000*max_delay_sec)].index
                        )

    # Add lags of X's
    if n_lags: 
        conds = [
            df["arbitrage_start"].shift(-lag+1).eq(1).fillna(False)
            for lag in range(n_lags+2)]
        
        # Since we deal with millions of rows, in would be problematic to shift and concat the whole df. Hence, we get rid of the rows we don't need anyway
        mask = np.logical_or.reduce(conds)
        df_filtered = df[mask]
        
        lagged_dfs = []
        
        filtered = df_filtered.drop(columns = without_lags, errors='ignore')
        logger.info(f"X's with {n_lags} lags: {filtered.columns}")
        
        for lag in range(1, n_lags+1):  
            df_shifted = filtered.shift(lag).add_suffix(f"_lag_{lag}")
            lagged_dfs.append(df_shifted)
            
        df = pd.concat([df_filtered, *lagged_dfs], axis=1)

    if include_first_observation:
        arbitrage_model = df[df.arbitrage_start == 1].copy()
        
    if not include_first_observation:
        df.length = df.length.shift(1) - df.ts + df.ts.shift(1)
        df[['till_arbitrage', 'since_last_arbitrage']] = df[['till_arbitrage', 'since_last_arbitrage']].shift(1)
        arbitrage_model = df[(df.arbitrage_start.shift(1) == 1) & (df.active_arbitrage == 1)].copy()
        
    arbitrage_model['dummy'] = (arbitrage_model['length'] >= 550).astype(int)
    arbitrage_model = arbitrage_model[~arbitrage_model.arbitrage_id.isin(id_to_exclude)]
    arbitrage_model.drop(columns = ['arbitrage_start', 'active_arbitrage', 'session_number'], errors='ignore', inplace = True)
    arbitrage_model.dropna(inplace = True)
    arbitrage_model.reset_index(inplace = True, drop = True)
    
    logger.info(f"DataFrame processed. Number of excluded observations: {len(id_to_exclude)}\
                \nExcluded id`s: {id_to_exclude}\
                \nAverage arbitrage duration: {arbitrage_model['length'].mean(): .0f}")
        
    return arbitrage_model
'''

def feature_selection(df: pl.DataFrame, exchange1: str, exchange2: str, roll: str = '10s') -> pl.DataFrame: 
    
    """
    Engineer snapshot and rolling‐window features from two merged orderbook streams.

    This routine takes a fully merged, time‐indexed Polars DataFrame of level‐book
    quotes and sizes for two exchanges, and produces a table of numeric predictors
    that capture both the instantaneous market state and recent dynamics over a
    specified rolling window.  It is intended as the preprocessing step before
    fitting a model to predict whether an arbitrage opportunity will persist.

    Parameters
    ----------
    df : pl.DataFrame (preprocessed)
    exchange1 : str
        Column‐name prefix for the “buy” side exchange (e.g. "HTX").
    exchange2 : str
        Column‐name prefix for the “sell” side exchange (e.g. "Bybit").
    roll : str, default "10s"
        A Polars‐compatible time interval (e.g. "10s", "500ms") over which to
        compute rolling summary statistics *prior* to each tick.

    Returns
    -------
    pl.DataFrame
        A new DataFrame with:
          - Instantaneous features:
              • level spreads on each exchange  
              • full‐book imbalance ratios  
              • top‐of‐book weight  
          - Rolling‐window aggregates (mean, std, etc.) of those same features
            computed over the preceding `roll` interval (excluding the current tick)
    """
    
    base = ['ts', "Bybit_delta_ts", 'HTX_delta_ts', 'timedelta', "arb_opportunity", 'length',
        'till_arbitrage', 'since_last_arbitrage', 'arbitrage_start', 'active_arbitrage', 'arbitrage_id']

    columns_to_roll = df.drop(base).columns

    df = df.with_columns(
    pl.col("ts").cast(pl.Datetime(time_unit='ms')))

    df = df.with_columns([
        pl.col(col).rolling_mean_by(
            "ts",
            window_size=roll,
            closed="left"
        ).alias(f"{col}_rolling_avg_{roll}") for col in columns_to_roll
    ])

    df = df.with_columns([
        pl.col(col).rolling_min_by(
        "ts",
        window_size=roll,
        closed="left"
        ).alias(f"{col}_rolling_min_{roll}") for col in columns_to_roll
    ])

    df = df.with_columns([
        pl.col(col).rolling_max_by(
        "ts",
        window_size=roll,
        closed="left"
        ).alias(f"{col}_rolling_max_{roll}") for col in columns_to_roll
    ])

    df_train = df.select([
        pl.col(
            base
            ),

        *[(pl.col(col) / pl.col(f"{col}_rolling_avg_{roll}") - 1)
        .alias(f"{col}_vs_avg") for col in columns_to_roll],

        *[(pl.col(col) / pl.col(f"{col}_rolling_min_{roll}") - 1)
        .alias(f"{col}_vs_min") for col in columns_to_roll],

        *[(pl.col(col) / pl.col(f"{col}_rolling_max_{roll}") - 1)
        .alias(f"{col}_vs_max") for col in columns_to_roll],


        # Spreads
        (pl.col(f"{exchange1}_level1_price_ask") - pl.col(f"{exchange1}_level1_price_bid")
        ).cast(pl.Float32).alias(f"{exchange1}_level1_bid_ask_spread"),
        (pl.col(f"{exchange1}_level2_price_ask") - pl.col(f"{exchange1}_level2_price_bid")
        ).cast(pl.Float32).alias(f"{exchange1}_level2_bid_ask_spread"),
        (pl.col(f"{exchange1}_level3_price_ask") - pl.col(f"{exchange1}_level3_price_bid")
        ).cast(pl.Float32).alias(f"{exchange1}_level3_bid_ask_spread"),

        (pl.col(f"{exchange1}_level2_price_ask") - pl.col(f"{exchange1}_level1_price_ask")
        ).cast(pl.Float32).alias(f"{exchange1}_level1_ask_spread"),
        (pl.col(f"{exchange1}_level3_price_ask") - pl.col(f"{exchange1}_level2_price_ask")
        ).cast(pl.Float32).alias(f"{exchange1}_level2_ask_spread"),

        (pl.col(f"{exchange1}_level1_price_bid") - pl.col(f"{exchange1}_level2_price_bid")
        ).cast(pl.Float32).alias(f"{exchange1}_level1_bid_spread"),
        (pl.col(f"{exchange1}_level2_price_bid") - pl.col(f"{exchange1}_level3_price_bid")
        ).cast(pl.Float32).alias(f"{exchange1}_level2_bid_spread"),

        (pl.col(f"{exchange2}_level1_price_ask") - pl.col(f"{exchange2}_level1_price_bid")
        ).cast(pl.Float32).alias(f"{exchange2}_level1_bid_ask_spread"),
        (pl.col(f"{exchange2}_level2_price_ask") - pl.col(f"{exchange2}_level2_price_bid")
        ).cast(pl.Float32).alias(f"{exchange2}_level2_bid_ask_spread"),
        (pl.col(f"{exchange2}_level3_price_ask") - pl.col(f"{exchange2}_level3_price_bid")
        ).cast(pl.Float32).alias(f"{exchange2}_level3_bid_ask_spread"),

        (pl.col(f"{exchange2}_level1_price_bid") - pl.col(f"{exchange2}_level2_price_bid")
        ).cast(pl.Float32).alias(f"{exchange2}_level1_bid_spread"),
        (pl.col(f"{exchange2}_level2_price_bid") - pl.col(f"{exchange2}_level3_price_bid")
        ).cast(pl.Float32).alias(f"{exchange2}_level2_bid_spread"),

        (pl.col(f"{exchange2}_level2_price_ask") - pl.col(f"{exchange2}_level1_price_ask")
        ).cast(pl.Float32).alias(f"{exchange2}_level1_ask_spread"),
        (pl.col(f"{exchange2}_level3_price_ask") - pl.col(f"{exchange2}_level2_price_ask")
        ).cast(pl.Float32).alias(f"{exchange2}_level2_ask_spread"),


        # Imbalance
        ((pl.col(f"{exchange1}_level1_size_bid") - pl.col(f"{exchange1}_level1_size_ask")) /
         (pl.col(f"{exchange1}_level1_size_bid") + pl.col(f"{exchange1}_level1_size_ask"))
        ).alias(f"{exchange1}_level1_imbalance"),

        ((pl.col(f"{exchange2}_level1_size_bid") - pl.col(f"{exchange2}_level1_size_ask")) /
         (pl.col(f"{exchange2}_level1_size_bid") + pl.col(f"{exchange2}_level1_size_ask"))
        ).alias(f"{exchange2}_level1_imbalance"),

        ((pl.col(f"{exchange1}_level1_size_bid") + pl.col(f"{exchange1}_level2_size_bid") + pl.col(f"{exchange1}_level3_size_bid") -
          pl.col(f"{exchange1}_level1_size_ask") - pl.col(f"{exchange1}_level2_size_ask") - pl.col(f"{exchange1}_level3_size_ask")) /
         (pl.col(f"{exchange1}_level1_size_bid") + pl.col(f"{exchange1}_level2_size_bid") + pl.col(f"{exchange1}_level3_size_bid") +
          pl.col(f"{exchange1}_level1_size_ask") + pl.col(f"{exchange1}_level2_size_ask") + pl.col(f"{exchange1}_level3_size_ask"))
        ).alias(f"{exchange1}_aggregated_imbalance"),

        ((pl.col(f"{exchange2}_level1_size_bid") + pl.col(f"{exchange2}_level2_size_bid") + pl.col(f"{exchange2}_level3_size_bid") -
          pl.col(f"{exchange2}_level1_size_ask") - pl.col(f"{exchange2}_level2_size_ask") - pl.col(f"{exchange2}_level3_size_ask")) /
         (pl.col(f"{exchange2}_level1_size_bid") + pl.col(f"{exchange2}_level2_size_bid") + pl.col(f"{exchange2}_level3_size_bid") +
          pl.col(f"{exchange2}_level1_size_ask") + pl.col(f"{exchange2}_level2_size_ask") + pl.col(f"{exchange2}_level3_size_ask"))
        ).alias(f"{exchange2}_aggregated_imbalance"),


        # Top-level quote weight
        (pl.col(f"{exchange1}_level1_size_bid") /
         (pl.col(f"{exchange1}_level1_size_bid") + pl.col(f"{exchange1}_level2_size_bid") + pl.col(f"{exchange1}_level3_size_bid"))
        ).alias(f"{exchange1}_bid_weight"),

        (pl.col(f"{exchange1}_level1_size_ask") /
         (pl.col(f"{exchange1}_level1_size_ask") + pl.col(f"{exchange1}_level2_size_ask") + pl.col(f"{exchange1}_level3_size_ask"))
        ).alias(f"{exchange1}_ask_weight"),

        (pl.col(f"{exchange2}_level1_size_bid") /
         (pl.col(f"{exchange2}_level1_size_bid") + pl.col(f"{exchange2}_level2_size_bid") + pl.col(f"{exchange2}_level3_size_bid"))
        ).alias(f"{exchange2}_bid_weight"),

        (pl.col(f"{exchange2}_level1_size_ask") /
         (pl.col(f"{exchange2}_level1_size_ask") + pl.col(f"{exchange2}_level2_size_ask") + pl.col(f"{exchange2}_level3_size_ask"))
        ).alias(f"{exchange2}_ask_weight")])

    df_train = df_train.with_columns(pl.col("ts").cast(pl.Int64))

    return df_train



def final_manipulations(
    df: pl.DataFrame,
    include_first_observation: bool = True,
    max_delay_sec: int = 60,
    n_lags: int | None = None,
    without_lags: list[str] = [
        'ts','active_arbitrage','arbitrage_start','session_number',
        'arbitrage_id','length','till_arbitrage','since_last_arbitrage',
        'Bybit_delta_ts','HTX_delta_ts','timedelta','night_hours'
    ],
) -> pl.DataFrame:
    """
    Perform the final round of cleaning and feature‐engineering on the merged arbitrage dataset.

    This function will:
      1. Optionally drop the very first arbitrage observation (since it is common to see arbitrages that last one tick).
      2. Remove any arbitrage events whose inter‐arrival delay exceeds `max_delay_sec`.
      3. Generate up to `n_lags` lagged versions of all numeric feature columns.
      4. Return a clean DataFrame ready for modelling.

    Parameters
    ----------
    df : pd.DataFrame
        The merged, time‐sorted data from both exchanges, including columns like
        “arbitrage_start”, per‐exchange `delta_ts`, and any other engineered features.
    include_first_observation : bool, default=True
        If False, drop the first arbitrage event tick that occurs.
        If True, keep those first‐after‐gap rows.
    max_delay_sec : int, default=60
        Any row whose delay since the previous arbitrage (in seconds) exceeds
        this threshold will be dropped. Helps remove spurious or stale events
        at session boundaries.
    n_lags : int or None, default=None
        If an integer, create lagged features up to `n_lags` periods for every
        numeric column (e.g. `feature`, `feature_lag_1`, …, `feature_lag_n_lags`).
        If None, no lagged features are added.
    without_lags : list, default = ['ts','active_arbitrage','arbitrage_start','session_number','arbitrage_id','length','till_arbitrage','since_last_arbitrage','Bybit_delta_ts','HTX_delta_ts','timedelta']
        List of variables, lags of which would not be added in the final model

    Returns
    -------
    pl.DataFrame
        A cleaned and fully feature‐engineered DataFrame, with:
          - unwanted session‐boundary rows removed,
          - excessive‐delay rows dropped,
          - optional lag‐features appended,
          - a fresh, zero‐based integer index.
    """
    # To start with, we need to exclude the very first and last arbitrage observation of each recording session, as it may bring bias
    df = df.with_columns([
        # flag a new session when gap > 5 minutes
        ((pl.col("ts") - pl.col("ts").shift(1)) > 5 * 60 * 1000)
        .cast(pl.Int32)
        .alias("is_session_break")
    ]).with_columns([
        # cumulative sum of breaks => session_number
        pl.col("is_session_break").cum_sum().alias("session_number")
    ]).drop("is_session_break")

    
    session_mins = df.group_by("session_number").agg(
        pl.col("arbitrage_id").min().alias("first_id")
    ).drop_nulls()

    first_ids = session_mins["first_id"].to_list()
    id_to_exclude = set(first_ids + [i - 1 for i in first_ids])

    # We also want to get rid of the data that may be recorded with delays
    delays = df.group_by("arbitrage_id").agg([
        pl.col("Bybit_delta_ts").max().alias("Bybit_max"),
        pl.col("HTX_delta_ts").max().alias("HTX_max"),
        pl.col("timedelta").abs().max().alias("td_max"),
    ])
    bad_ids = (
        delays
        .filter(
            (pl.col("Bybit_max") > 1000 * max_delay_sec) |
            (pl.col("HTX_max"  ) > 1000 * max_delay_sec) |
            (pl.col("td_max"   ) > 1000 * max_delay_sec)
        )
        .get_column("arbitrage_id")
        .drop_nulls()
        .to_list()
    )
    id_to_exclude.update(bad_ids)

    # Add lags of X's
    if n_lags:
        # build a mask that keeps all ticks up to n_lags
        conds = [
            pl.col("arbitrage_start")
              .shift(-lag + 1)
              .eq(1)
              .fill_null(False)
            for lag in range(n_lags + 2)
        ]
        # Since we deal with millions of rows, in would be problematic to shift and concat the whole df. 
        # Hence, we get rid of the rows we don't need anyway
        mask = reduce(lambda a, b: a | b, conds)
        df_filtered = df.filter(mask)
        
        to_lag = [c for c in df_filtered.columns if c not in without_lags]
        base = df_filtered.select(to_lag)

        # build each lag‐frame and rename its cols
        lag_frames = []
        for lag in range(1, n_lags + 1):
            # Create a list of Series from the shifted base DataFrame
            shifted_series = base.shift(lag).rename({c: f"{c}_lag_{lag}" for c in to_lag}).get_columns()
            lag_frames.extend(shifted_series) # Extend the list with the new Series
        # horizontally stack the original filtered + all lags
        df = df_filtered.hstack(lag_frames)

    # pick which ticks to keep
    if include_first_observation:
        arb = df.filter(pl.col("arbitrage_start") == 1)
    else:
        arb = (
            df
            .with_columns([
                # recompute length & time‐since metrics lagged by 1
                ((pl.col("length").shift(1) - pl.col("ts") + pl.col("ts").shift(1)))
                  .alias("length"),
                pl.col("till_arbitrage").shift(1).alias("till_arbitrage"),
                pl.col("since_last_arbitrage").shift(1).alias("since_last_arbitrage"),
            ])
            .filter(
                (pl.col("arbitrage_start").shift(1) == 1) &
                (pl.col("active_arbitrage") == 1)
            )
        )
    # dummy label
    arb = arb.with_columns([
        (pl.col("length") >= 550).cast(pl.Int8).alias("dummy")
    ])

      # final filtering & cleanup
    arb = (
        arb
        # drop excluded arbitrage_id’s
        .filter(~pl.col("arbitrage_id").is_in(list(id_to_exclude)))
        # drop helper cols
        .drop([c for c in ["arbitrage_start","active_arbitrage","session_number"]
               if c in arb.columns])
        # drop any rows with nulls
        .drop_nulls()
    )
    
    logger.info(f"DataFrame processed. Number of excluded observations: {len(id_to_exclude)}\
                \nExcluded id`s: {id_to_exclude}\
                \nAverage arbitrage duration: {arb['length'].mean(): .0f}")
                
    return arb