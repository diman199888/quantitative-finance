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
from tqdm import tqdm


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
    Identify arbitrage windows in a tick-level time series and compute their start/end markers.

    For each row in `df`, which must contain:
      - `ts`: a monotonically increasing timestamp (e.g. in milliseconds)
      - `arb_opportunity`: the current arbitrage spread

    this function will:

    1. **Flag window boundaries**
       - `arbitrage_start` (bool): True on the first tick where `arb_opportunity` crosses zero.
       - `active_arbitrage` (bool): True on every tick while |`arb_opportunity`| ≥ 0.0.
       - `arbitrage_opportunity` (datetime): True on every tick while |`arb_opportunity`| ≥ 0.03.


    2. **Assign window IDs**
       - `arbitrage_id` (int): a running counter that increases by one on each `arbitrage_start`,
         but only during active windows.
       - `has_opportunity` (int): ID's of interest (arbitrage_id with arbitrage_opportunity)
         
    3. **Select id's with arbitrage_opportunity and merge them back**

    4. **Compute relative times**
       - `till_arbitrage`: `ts – arbitrage_start`, the elapsed time since the window opened.
       - `since_last_arbitrage`: `ts – arbitrage_opportunity`, the elapsed time since the last window closed.
       - `since_last_discrepancy`: `ts – active_arbitrage`, the elapsed time since the last discrepancy (non-zero value of arb_opportunity) closed.
       
    Returns
    -------
    pl.DataFrame
        A new DataFrame with all the original columns plus:
          - `arbitrage_opportunity`
          - `arbitrage_id`
          - `till_arbitrage`, `since_last_arbitrage`, `since_last_discrepancy`
    """

    df_n = df.with_columns([
        # Flag arbitrage start
        (
            (
                (pl.col("arb_opportunity") > 0.0) & (pl.col("arb_opportunity").shift(1) == 0.0)
            ) | (
                (pl.col("arb_opportunity") < -0.0) & (pl.col("arb_opportunity").shift(1) == 0.0)
            )
        ).alias("arbitrage_start"),
        ((pl.col("arb_opportunity") > 0.0) | (pl.col("arb_opportunity") < -0.0)).alias("active_arbitrage"),
        ((pl.col("arb_opportunity") >= 0.03) | (pl.col("arb_opportunity") <= -0.03)).alias("arbitrage_opportunity"),
    ])

    # Assign arbitrage_id by cumulative sum of starts
    df_n = df_n.with_columns(

        (pl.when(pl.col("active_arbitrage"))
        .then(pl.cum_sum("arbitrage_start"))
        .otherwise(None)
        .alias("arbitrage_id")))

    active_groups = df_n.group_by("arbitrage_id").agg(
                              pl.col("arbitrage_opportunity")
                                .any()
                                .alias("has_opportunity")
                            ).filter(pl.col("has_opportunity"))


    # Assign the final arbitrage_id only to the valid active groups
    df_n = df_n.join(active_groups, on="arbitrage_id", how="left")

    df_n = df_n.with_columns([
    (pl.col("arbitrage_start") & pl.col("has_opportunity"))
    .fill_null(False)
    .alias("opportunity_start")
    ])
    df_n = df_n.with_columns([
        pl.when(pl.col("has_opportunity"))
        .then(pl.cum_sum("opportunity_start"))
        .otherwise(None)
        .alias("arbitrage_id")])

    df_n = df_n.with_columns([

        (pl.when(pl.col("arbitrage_start"))
        .then(pl.col("ts"))
        .fill_null(strategy="forward")
        .alias('till_arbitrage')),

        (pl.when((pl.col("arbitrage_opportunity").shift(1) == True) & (pl.col("arbitrage_opportunity") == False))
        .then(pl.col("ts").shift(1))
        .fill_null(strategy="forward")
        .alias("since_last_arbitrage")),

        (pl.when(
            (
                (pl.col("arb_opportunity") > 0.0).shift(1) | (pl.col("arb_opportunity") < -0.0).shift(1)
            )
                & (pl.col("arb_opportunity") == 0.0)
                # & (pl.col("arbitrage_id").shift(1).is_null())
            )
        .then(pl.col("ts").shift(1))
        .fill_null(strategy="forward")
        .alias("since_last_discrepancy")),
    ])

    df_n = df_n.with_columns([

        (pl.when(pl.col("arbitrage_opportunity"))
        .then(pl.col("ts") - pl.col("till_arbitrage"))
        .alias('till_arbitrage')),

        (pl.when(pl.col("arbitrage_opportunity"))
        .then(pl.col("ts") - pl.col("since_last_arbitrage"))
        .alias("since_last_arbitrage")),

        (pl.when(pl.col("arbitrage_opportunity"))
        .then(pl.col("ts") - pl.col("since_last_discrepancy"))
        .alias("since_last_discrepancy"))
    ]).drop(["has_opportunity", "arbitrage_start",	"opportunity_start",	"active_arbitrage"])
    
    logger.info(f"DataFrame processed. \
                \nNumber of arbitrages: {df_n['arbitrage_id'].max():,d}\
                ")


    return df_n.unique(maintain_order = True, keep = 'first')

def expected_prices(df_n, threshold = 100):
    '''
    Support function to return the dataframe consisting of prices and sizes that are used to place orders
    
    '''
    mask = pl.when(pl.col("arb_opportunity") > 0) \
            .then((pl.col("HTX_level1_size_ask") * pl.col("HTX_level1_price_ask") > threshold) & (pl.col("Bybit_level1_size_bid") * pl.col("Bybit_level1_price_bid") > threshold)) \
            .otherwise(
              pl.when(pl.col("arb_opportunity") < 0).then(pl.col("HTX_level1_size_bid") * pl.col("HTX_level1_price_bid") > threshold) & (pl.col("Bybit_level1_size_ask") * pl.col("Bybit_level1_price_ask") > threshold)
              )
  
    first_ts = df_n.group_by("arbitrage_id").agg(
        pl.col("ts")
        .filter(pl.col("arbitrage_opportunity") & mask)
        .first()
        .alias("first_ts")
    ).drop_nulls()
  
    expected_df = df_n.filter(
        pl.col('ts')
        .is_in(first_ts
              .select(pl.col("first_ts"))
              .to_numpy()
              .flatten())) \
        .select(pl.col('ts'),
  
            pl.when(pl.col("arb_opportunity") > 0.0)
            .then(pl.col('HTX_level1_price_ask'))
            .otherwise(pl.when(pl.col("arb_opportunity") < 0.0).then(pl.col('Bybit_level1_price_ask')))
            .alias('buy_price'),
  
            pl.when(pl.col("arb_opportunity") > 0.0)
            .then(pl.col('HTX_level1_size_ask'))
            .otherwise(pl.when(pl.col("arb_opportunity") < 0.0).then(pl.col('Bybit_level1_size_ask')))
            .alias('buy_size'),
  
            pl.when(pl.col("arb_opportunity") > 0.0)
            .then(pl.col('Bybit_level1_price_bid'))
            .otherwise(pl.when(pl.col("arb_opportunity") < 0.0).then(pl.col('HTX_level1_price_bid')))
            .alias('sell_price'),
  
            pl.when(pl.col("arb_opportunity") > 0.0)
            .then(pl.col('Bybit_level1_size_bid'))
            .otherwise(pl.when(pl.col("arb_opportunity") < 0.0).then(pl.col('HTX_level1_size_bid')))
            .alias('sell_size'),
  
            pl.when(pl.col("arb_opportunity") > 0.0)
            .then(pl.lit('bHsB'))
            .otherwise(pl.lit('bBsH'))
            .alias('strategy')
        ).drop_nulls()
        #.unique(subset = "ts", keep = "first")
  
    return expected_df

def check(row, df_n, investment = 200, start_time = 500, open_order_minutes = 1):
    '''
    Support function that checks whether expected prices and sizes are still available start_time ms later from order placement
    '''
    ts = row[0]
    expected_buy_price = row[1]
    expected_buy_size = row[2]
    expected_sell_price = row[3]
    expected_sell_size = row[4]
    strategy = row[5]

    expected_buy_liquidity = expected_buy_price * expected_buy_size
    expected_sell_liquidity = expected_sell_price * expected_sell_size
    expected_arbitrage_size = min(investment, expected_buy_liquidity, expected_sell_liquidity)

    filtered_df = df_n.filter(
        (pl.col('ts') >= ts + start_time) & (pl.col('ts') <= ts + open_order_minutes*60*1000)
    )

    if strategy == "bHsB":
        buy_exchange = "HTX"
        sell_exchange = "Bybit"
    elif strategy == "bBsH":
        buy_exchange = "Bybit"
        sell_exchange = "HTX"

    if filtered_df.is_empty():
        return False

    buy_price_matrix = filtered_df.select(pl.col(f'{buy_exchange}_level{i}_price_ask' for i in (1,2,3))).to_numpy()
    buy_size_matrix = filtered_df.select(pl.col(f'{buy_exchange}_level{i}_size_ask' for i in (1,2,3))).to_numpy()
    sell_price_matrix = filtered_df.select(pl.col(f'{sell_exchange}_level{i}_price_bid' for i in (1,2,3))).to_numpy()
    sell_size_matrix = filtered_df.select(pl.col(f'{sell_exchange}_level{i}_size_bid' for i in (1,2,3))).to_numpy()

    buy_mask = buy_price_matrix <= expected_buy_price
    sell_mask = sell_price_matrix >= expected_sell_price

    if not np.any(buy_mask) or not np.any(sell_mask):
        return False

    buy_matrix_1 = buy_price_matrix[buy_mask]
    buy_matrix_2 = buy_size_matrix[buy_mask]
    buy_index = np.argmax(buy_matrix_2)
    buy_matrix = np.c_[buy_matrix_1, buy_matrix_2][buy_index, :]

    sell_matrix_1 = sell_price_matrix[sell_mask]
    sell_matrix_2 = sell_size_matrix[sell_mask]
    sell_index = np.argmax(sell_matrix_2)
    sell_matrix = np.c_[sell_matrix_1, sell_matrix_2][sell_index, :]


    statement = ((buy_matrix[0] * buy_matrix[1] >= expected_arbitrage_size) and (sell_matrix[0] * sell_matrix[1] >= expected_arbitrage_size))
    return statement

def df_with_dummy(expected_df, df):
    '''
    Main function that returns dataframe with the dummy variable: 1 if the arbitrage opportunity is persistent and 0 otherwise
    '''
    expected_df_with_dummy = expected_df.select(pl.col("ts"),
        pl.Series(expected_df.map_rows(lambda row: check(row, df), return_dtype = pl.Int64)).alias("dummy")
        )
    df_with_dummy = df.join(expected_df_with_dummy, on = "ts", how="left")
  
    return df_with_dummy


def feature_selection(df: pl.DataFrame, exchange1: str, exchange2: str, roll: str = '45s') -> pl.DataFrame:

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
    base = ["ts", 'HTX_delta_ts', 'Bybit_delta_ts', 'timedelta', 'arb_opportunity', 'arbitrage_id', 'dummy', 'arbitrage_opportunity',
            'till_arbitrage', 'since_last_arbitrage', 'since_last_discrepancy']

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
            ["ts", 'HTX_delta_ts', 'Bybit_delta_ts', 'timedelta', 'arb_opportunity', 'arbitrage_id', 'dummy',
             'till_arbitrage', 'since_last_arbitrage', 'since_last_discrepancy',
             'HTX_level1_size_bid', 'Bybit_level1_size_bid','HTX_level1_size_ask','Bybit_level1_size_ask']
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
        ).alias(f"{exchange2}_ask_weight"),


        ])

    return df_train



def final_manipulations(
    df: pl.DataFrame,
    max_delay_sec: int = 60,
    n_lags: int | None = None,
    without_lags: list[str] = [
        'ts','active_arbitrage','arbitrage_start','session_number',
        'arbitrage_id','length','till_arbitrage','since_last_arbitrage',
        'Bybit_delta_ts','HTX_delta_ts','timedelta','night_hours', 'dummy'
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
    
    df = df.drop(["session_number"])

    # Add lags of X's
    if n_lags:
        # build a mask that keeps all ticks up to n_lags
        conds = [
            pl.col("dummy")
              .shift(-lag)
              .is_not_null()
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


    arb = df.filter(pl.col("dummy").is_not_null())

      # final filtering & cleanup
    arb = (
        arb
        # drop excluded arbitrage_id’s
        .filter(~pl.col("arbitrage_id").is_in(list(id_to_exclude)))
        # drop helper cols
        .drop([c for c in ["arbitrage_start","active_arbitrage"]
               if c in arb.columns])
        # drop any rows with nulls
        .drop_nulls()
    )

    logger.info(f"DataFrame processed. Number of excluded observations: {len(id_to_exclude)}\
                \nExcluded id`s: {id_to_exclude}")

    return arb.unique(subset=["ts"], maintain_order=True, keep = "last")



###############################################################################
################## PART III: BACKTESTING ######################################
###############################################################################


def matrices(
    expected_df: pl.DataFrame,
    realized_df_buy: pl.DataFrame,
    realized_df_sell: pl.DataFrame,
    buy_exchange: str,
    sell_exchange: str,
    buy_cycle_investment: int,
    sell_cycle_investment: int,
    threshold: int,
    buy_ts_list: np.ndarray = np.array([]),
    sell_ts_list: np.ndarray = np.array([]),
  ):
  '''
  This support function is used to form matrices of price-size pairs that satisfy our conditions:
  open orders remain outstanding till either the correspoding offsetting deal is met during the trading session or till the end of the trading session.
  '''
  # Prices and sizes that are used for decision-making
  expected_buy_price = expected_df.select(pl.col(f'{buy_exchange}_level1_price_ask')).item()
  available_buy_size = expected_df.select(pl.col(f'{buy_exchange}_level1_size_ask')).item()

  expected_sell_price = expected_df.select(pl.col(f'{sell_exchange}_level1_price_bid')).item()
  available_sell_size = expected_df.select(pl.col(f'{sell_exchange}_level1_size_bid')).item()

  # check liquidity volumes
  expected_buy_liquidity = expected_buy_price * available_buy_size
  expected_sell_liquidity = expected_sell_price * available_sell_size
  expected_buy_arbitrage_size = min(buy_cycle_investment, expected_buy_liquidity, expected_sell_liquidity)
  expected_sell_arbitrage_size = min(sell_cycle_investment, expected_buy_liquidity, expected_sell_liquidity)

  if expected_buy_arbitrage_size >= threshold: # we are not interested in placing orders smaller than a certain amount

    expected_buy_size = round(expected_buy_arbitrage_size / expected_buy_price, 2)
    expected_sell_size = round(expected_sell_arbitrage_size / expected_buy_price, 2)

    # We assume that if order is not executed immediately, it remains open till the end of the trading session. Here we form the matrices of all the potential deals
    buy_price_matrix = realized_df_buy.select(pl.col(f'{buy_exchange}_level{i}_price_ask' for i in range(1,4))).to_numpy()
    buy_size_matrix = realized_df_buy.select(pl.col(f'{buy_exchange}_level{i}_size_ask' for i in range(1,4))).to_numpy()

    sell_price_matrix = realized_df_sell.select(pl.col(f'{sell_exchange}_level{i}_price_bid' for i in range(1,4))).to_numpy()
    sell_size_matrix = realized_df_sell.select(pl.col(f'{sell_exchange}_level{i}_size_bid' for i in range(1,4))).to_numpy()

    buy_matrix_1 = buy_price_matrix[buy_price_matrix <= expected_buy_price]
    buy_matrix_2 = buy_size_matrix[buy_price_matrix <= expected_buy_price]
    buy_matrix = np.c_[buy_matrix_1, buy_matrix_2]

    # we want to get rid of duplicates
    _, unique_indices = np.unique(buy_matrix, axis=0, return_index=True)
    buy_matrix = buy_matrix[np.sort(unique_indices)]

    buy_ts = np.repeat(realized_df_buy.select(pl.col('ts')).to_numpy(), 3, axis = 1)
    buy_ts = buy_ts[buy_price_matrix <= expected_buy_price]
    buy_ts = buy_ts[np.sort(unique_indices)]


    buy_matrix = buy_matrix[~np.isin(buy_ts, buy_ts_list)]  #exclude observations that were used in previous cycles
    buy_ts = buy_ts[~np.isin(buy_ts, buy_ts_list)]


    sell_matrix_1 = sell_price_matrix[sell_price_matrix >= expected_sell_price]
    sell_matrix_2 = sell_size_matrix[sell_price_matrix >= expected_sell_price]
    sell_matrix = np.c_[sell_matrix_1, sell_matrix_2]

    _, unique_indices = np.unique(sell_matrix, axis=0, return_index=True)
    sell_matrix = sell_matrix[np.sort(unique_indices)]

    sell_ts = np.repeat(realized_df_sell.select(pl.col('ts')).to_numpy(), 3, axis = 1)
    sell_ts = sell_ts[sell_price_matrix >= expected_sell_price]
    sell_ts = sell_ts[np.sort(unique_indices)]

    sell_matrix = sell_matrix[~np.isin(sell_ts, sell_ts_list)]
    sell_ts = sell_ts[~np.isin(sell_ts, sell_ts_list)]

    return buy_matrix, sell_matrix, buy_ts, sell_ts, expected_sell_arbitrage_size, expected_buy_size, expected_sell_size, expected_buy_price, expected_sell_price, available_buy_size, available_sell_size

  else:
    logger.info(f'low liquidity: ${expected_sell_arbitrage_size: .2f}')
    return



def match_volume(mat: np.ndarray, target_volume: float, ts = None) -> np.ndarray:
  '''
  This support function is used to match available open positions with the expected order size or to match volumes between orders at two exchanges:
  it can be the case that at one exchange the volumes are enough to execute the (expected) order,
  while on the other exchange the price or volume might have changed faster than the order was placed.
  Hence, all the coins beyond offsetting deals should be considered as inventories and kept till the end of the trading session, when they are sold at the market price
  '''
  if target_volume == 0:
    return np.zeros((0, 2))

  prices = mat[:, 0]
  sizes  = mat[:, 1]

  # 1) compute cumulative sum of sizes
  cumsum_sizes = np.cumsum(sizes)

  # 2) find the first index where cumsum_sizes >= target_volume
  idx = np.searchsorted(cumsum_sizes, target_volume, side="left")

  if idx >= len(mat):
      # All levels are needed, but the total sum could be <= target_volume.
      # In that case, just return the entire matrix.
      if ts is not None:
          return mat.copy(), ts.copy()
      else:
          return mat.copy()

  # 3) build the trimmed matrix: take all full levels BEFORE idx,
  #    and a partial level at idx so that we hit exactly target_volume.
  if idx == 0:
      # Even the very first level already exceeds target_volume,
      # so we only take part of the first row.
      truncated_size = target_volume
      new_rows = np.array([[prices[0], truncated_size]])
      if ts is not None:
          return new_rows, ts[0]
      else:
          return new_rows

  # Otherwise, we take all levels 0..idx-1 in full, and a partial of level idx
  full_rows      = mat[:idx]  # all rows before idx
  vol_before_idx = cumsum_sizes[idx - 1]  # cumulative volume up to (idx-1)

  # how much volume we still need at level idx
  expected_vol = target_volume - vol_before_idx
  # prices[idx] is the price at that level; we allocate expected_vol volume there:
  last_row = np.array([[prices[idx], expected_vol]])

  # concatenate
  if ts is not None:
      return np.vstack([full_rows, last_row]), ts[:idx+1]
  else:
      return np.vstack([full_rows, last_row])

def build_inventory_matrix(full_matrix: np.ndarray, matched: np.ndarray) -> np.ndarray:
  '''
  support function to calculate outstanding inventories
  '''
  if full_matrix.shape[0] == 0: #no inventories
      return np.zeros((0, 2))
  if matched.size == 0:
      return full_matrix
  # number of fully matched rows is matched.shape[0]
  k = matched.shape[0]
  # subtract matched volumes from the first k rows:
  support = full_matrix[:k].copy()
  support[:,1] = support[:,1] - matched[:,1]

  # if there is a “next” level that was partially used, append it:
  if k < full_matrix.shape[0]:
      support = np.vstack([support, full_matrix[k:]])
  return support


def backtesing(trained_model, scaler, arbitrage_model, df_n, trading_hours, HTX_execution_time, Bybit_execution_time, capital, investment, threshold, start_timestamp_ms):
  '''
  The main function to backtest different models by calculating the PnL.
  '''
  # Set logger module to save data in a log file
  # 1) Grab the root logger (or grab a named one if you created one in your notebook)
  root = logging.getLogger()  
  root.setLevel(logging.INFO)

  # 2) Remove all existing handlers (so nothing logs to the notebook)
  for h in root.handlers[:]:
      root.removeHandler(h)

  # 3) Create a FileHandler that writes to /content/backtest.log
  fh = logging.FileHandler("backtest.log", mode="a")
  fh.setLevel(logging.INFO)
  fh.setFormatter(
      logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
  )
  root.addHandler(fh)

  total_missed_opportunity = 0
  total_expected_profit = 0
  total_realized_profit = 0
  buy_ts_list = np.array([])
  sell_ts_list = np.array([])
  number_of_bBsH = 0
  number_of_bHsB = 0
  HTX_funds = capital*0.95 #since most of the buy deals are happening at HTX, we allocate more capital there
  Bybit_funds = capital*0.05
  LTC_fee = 0.001
  USDt_fee = 1

  # Calculate the end timestamp (hours later in milliseconds)
  duration_ms = trading_hours * 60 * 60 * 1000
  end_timestamp_ms = start_timestamp_ms + duration_ms

  # Filter the DataFrame for the 24-hour period
  df_filtered = df_n.filter(
      (pl.col('ts') >= start_timestamp_ms) & (pl.col('ts') <= end_timestamp_ms)
  )

  start = df_filtered.select(pl.col('ts'))[0].item()
  end = df_filtered.select(pl.col('ts'))[-1].item()

  row_start = arbitrage_model[arbitrage_model.ts >= start].index[0]
  row_end = arbitrage_model[arbitrage_model.ts <= end].index[-1]

  X = arbitrage_model.drop(columns = ['ts', 'dummy', 'arbitrage_id', 'HTX_delta_ts', 'Bybit_delta_ts', 'timedelta'])
  y = arbitrage_model["dummy"]

  sleep = 0

  # Predict values row by row
  for pos in tqdm(range(row_start, row_end)):

    if sleep:
      logger.info(f'sleeping for {sleep} rows')
      sleep -= 1
      continue

    logger.info(f'Row number: {pos} out of {row_end}')
    now = time.time()

    X_scaled = scaler.transform(X.iloc[pos:pos+1])
    prediction = trained_model.predict(X_scaled)[0]
    true_value = y.iloc[pos]
    logger.info(f'prediction: {prediction};\ttrue value: {true_value}')
    prediction_time = time.time() - now
    if prediction_time > 0.05:
      logger.warning(f'prediction time: {prediction_time: .2f}')

    ts = arbitrage_model.loc[pos, 'ts']

    # the tick info we use to predict the outcome
    expected_df = df_filtered.filter(pl.col('ts') == ts)[0]

    # the most
    realized_df_HTX = (
      df_filtered
      .filter(pl.col('ts') >= (ts + HTX_execution_time + prediction_time),
              pl.col('ts') <= (ts + 60000)) #keep order open for 1 minute
    )
    realized_df_Bybit = (
      df_filtered
      .filter(pl.col('ts') >= (ts + Bybit_execution_time + prediction_time),
              pl.col('ts') <= (ts + 60000))
    )

    if prediction == 1:

      if expected_df.select(pl.col('arb_opportunity')).item() > 0: #buy at HTX, sell at Bybit
        strategy = "bHsB"

        if HTX_funds >= investment:
          buy_cycle_investment = sell_cycle_investment = investment
        else:
          buy_cycle_investment = HTX_funds
          sell_cycle_investment = buy_cycle_investment - LTC_fee*expected_df.select(pl.col('HTX_level1_price_ask')).item()

        try:
          buy_matrix, sell_matrix, buy_ts_matrix, sell_ts_matrix, expected_arbitrage_size, expected_buy_size, expected_sell_size, expected_buy_price, expected_sell_price, available_buy_size, available_sell_size = matrices(
              expected_df = expected_df,
              realized_df_buy = realized_df_HTX,
              realized_df_sell = realized_df_Bybit,
              buy_exchange = 'HTX',
              sell_exchange = 'Bybit',
              buy_cycle_investment = buy_cycle_investment,
              sell_cycle_investment = sell_cycle_investment,
              threshold = threshold,
              buy_ts_list = buy_ts_list,
              sell_ts_list = sell_ts_list)
          number_of_bHsB += 1

        except Exception as e:
            logger.warning(f'error: {e}')
            continue


      elif expected_df.select(pl.col('arb_opportunity')).item() < 0:
        strategy = "bBsH"

        if Bybit_funds >= investment:
          buy_cycle_investment = sell_cycle_investment = investment
        else:
          buy_cycle_investment = Bybit_funds
          sell_cycle_investment = buy_cycle_investment - LTC_fee*expected_df.select(pl.col('Bybit_level1_price_ask')).item()

        try:
          buy_matrix, sell_matrix, buy_ts_matrix, sell_ts_matrix, expected_arbitrage_size, expected_buy_size, expected_sell_size, expected_buy_price, expected_sell_price, available_buy_size, available_sell_size = matrices(
              expected_df = expected_df,
              realized_df_buy = realized_df_Bybit,
              realized_df_sell = realized_df_HTX,
              buy_exchange = 'Bybit',
              sell_exchange = 'HTX',
              buy_cycle_investment = buy_cycle_investment,
              sell_cycle_investment = sell_cycle_investment,
              threshold = threshold,
              buy_ts_list = buy_ts_list,
              sell_ts_list = sell_ts_list)
          number_of_bBsH += 1

        except Exception as e:
            logger.warning(f'error: {e}')
            continue

      logger.info(f'{expected_arbitrage_size}, {expected_buy_size}, {expected_sell_size}')

      if buy_matrix.size != 0:
        realized_buy_matrix, buy_ts = match_volume(buy_matrix, expected_buy_size, buy_ts_matrix)
        realized_buy_size = np.sum(realized_buy_matrix[:,1])
        HTX_funds -= realized_buy_matrix[:,0] @ realized_buy_matrix[:,1] if strategy == "bHsB" else 0
        Bybit_funds -= realized_buy_matrix[:,0] @ realized_buy_matrix[:,1] if strategy == "bBsH" else 0
      else:
        realized_buy_matrix = np.zeros((0, 2))
        realized_buy_size = 0
        buy_ts = np.nan

      if sell_matrix.size != 0:
        realized_sell_matrix, sell_ts = match_volume(sell_matrix, expected_sell_size, sell_ts_matrix)
        realized_sell_size = np.sum(realized_sell_matrix[:,1])
        HTX_funds += realized_sell_matrix[:,0] @ realized_sell_matrix[:,1] if strategy == "bBsH" else 0
        Bybit_funds += realized_sell_matrix[:,0] @ realized_sell_matrix[:,1] if strategy == "bHsB" else 0
      else:
        realized_sell_matrix = np.zeros((0, 2))
        realized_buy_size = 0
        sell_ts = np.nan

      if realized_buy_size == realized_sell_size:
          buy_matched  = realized_buy_matrix
          sell_matched = realized_sell_matrix
          HTX_inventory_drawdown = 0
          Bybit_inventory_drawdown = 0
      else:
          realized_size = min(realized_buy_size, realized_sell_size)
          buy_matched  = match_volume(realized_buy_matrix,  realized_size)
          sell_matched = match_volume(realized_sell_matrix, realized_size)
          inventory_buy_matrix  = build_inventory_matrix(realized_buy_matrix,  buy_matched)
          inventory_sell_matrix = build_inventory_matrix(realized_sell_matrix, sell_matched)

          if strategy == "bHsB":

            HTX_last_price = realized_df_HTX.select(pl.col('HTX_level1_price_bid'))[-1].item()
            HTX_inventory_drawdown = (HTX_last_price - inventory_buy_matrix[:,0]) @ inventory_buy_matrix[:,1]

            Bybit_last_price = realized_df_Bybit.select(pl.col('Bybit_level1_price_ask'))[-1].item()
            Bybit_inventory_drawdown = (inventory_sell_matrix[:,0] - Bybit_last_price) @ inventory_sell_matrix[:,1]

            HTX_funds += HTX_inventory_drawdown + inventory_buy_matrix[:,0] @ inventory_buy_matrix[:,1]
            Bybit_funds += Bybit_inventory_drawdown - inventory_sell_matrix[:,0] @ inventory_sell_matrix[:,1]


          elif strategy == "bBsH":

            HTX_last_price = realized_df_HTX.select(pl.col('HTX_level1_price_ask'))[-1].item()
            HTX_inventory_drawdown = (inventory_sell_matrix[:,0] - HTX_last_price) @ inventory_sell_matrix[:,1]

            Bybit_last_price = realized_df_Bybit.select(pl.col('Bybit_level1_price_bid'))[-1].item()
            Bybit_inventory_drawdown = (Bybit_last_price - inventory_buy_matrix[:,0]) @ inventory_buy_matrix[:,1]

            HTX_funds += HTX_inventory_drawdown - inventory_sell_matrix[:,0] @ inventory_sell_matrix[:,1]
            Bybit_funds += Bybit_inventory_drawdown + inventory_buy_matrix[:,0] @ inventory_buy_matrix[:,1]


      buy_ts_list = np.append(buy_ts_list, buy_ts)
      sell_ts_list = np.append(sell_ts_list, sell_ts)

      realized_profit = sell_matched[:, 0] @ sell_matched[:, 1] - buy_matched[:, 0] @ buy_matched[:, 1] + HTX_inventory_drawdown + Bybit_inventory_drawdown
      total_realized_profit += realized_profit

      expected_profit = expected_arbitrage_size * (expected_sell_price/expected_buy_price - 1)
      total_expected_profit += expected_profit
      
      logger.info(f"""Expected values:
      actual ts: {ts}; {pd.to_datetime(ts, unit='ms')},
      buy price, $: {expected_buy_price}, buy size, LTC: {available_buy_size}
      sell price, $: {expected_sell_price}, sell size, LTC: {available_sell_size}
      profit, $: {expected_profit:.2f}
      """)

      logger.info(f"""Realized values:
      buy ts: {buy_ts}; {pd.to_datetime(buy_ts, unit='ms')}, buy ts diff: {buy_ts - ts},
      sell ts: {sell_ts}; {pd.to_datetime(sell_ts, unit='ms')}, sell ts diff: {sell_ts - ts}
      realized buy matrix: {realized_buy_matrix}, realized sell matrix: {realized_sell_matrix}
      matched buy matrix: {buy_matched}, matched sell matrix: {sell_matched}
      profit, $: {realized_profit:.2f}
      """)
      
      logger.info(f'HTX funds, $: {HTX_funds: .2f}; Bybit funds, $: {Bybit_funds: .2f}')
      logger.info(f'total funds excess, $: {HTX_funds + Bybit_funds - capital: .2f}')
      logger.info(f'total realized profit, $: {total_realized_profit: .2f}')
      
      assert round(HTX_funds + Bybit_funds - capital, 2) == round(total_realized_profit, 2)  #sanity check


      if (Bybit_funds <= threshold) or (HTX_funds <= threshold):
        logger.info(f'insufficient funds')
        # Check if there are future timestamps before accessing the index
        future_trades = arbitrage_model[(arbitrage_model.ts >= ts + 10*60*1000)]
        if not future_trades.empty:
            sleep = future_trades.index[0] - pos  #10 minutes to transfer funds back and forth
            logger.info(f'sleeping for {sleep} rows')
            transfer_costs = LTC_fee*expected_buy_price + USDt_fee #It costs 0.001 LTC to transfer LTC (which will be used to close shorts) and then 1usdt to transfer tether back
            logger.info(f'transfer costs, $: {transfer_costs: .2f}')
            total_realized_profit -= transfer_costs
            new_capital = HTX_funds + Bybit_funds - transfer_costs
            logger.info(f'new capital, $: {new_capital: .2f}')
            HTX_funds = new_capital*0.95
            Bybit_funds = new_capital*0.05
            continue
        else:
            break

    if prediction == 0 and true_value == 1:
      if expected_df.select(pl.col('arb_opportunity')).item() > 0:

        realized_buy_price = realized_df_HTX.select(pl.col('HTX_level1_price_ask'))[0].item()
        realized_buy_size = realized_df_HTX.select(pl.col('HTX_level1_size_ask'))[0].item()

        realized_sell_price = realized_df_Bybit.select(pl.col('Bybit_level1_price_bid'))[0].item()
        realized_sell_size = realized_df_Bybit.select(pl.col('Bybit_level1_size_bid'))[0].item()

      if expected_df.select(pl.col('arb_opportunity')).item() < 0:

        realized_buy_price = realized_df_Bybit.select(pl.col('Bybit_level1_price_ask'))[0].item()
        realized_buy_size = realized_df_Bybit.select(pl.col('Bybit_level1_size_ask'))[0].item()

        realized_sell_price = realized_df_HTX.select(pl.col('HTX_level1_price_bid'))[0].item()
        realized_sell_size = realized_df_HTX.select(pl.col('HTX_level1_size_bid'))[0].item()


      realized_buy_liquidity = realized_buy_price * realized_buy_size
      realized_sell_liquidity = realized_sell_price * realized_sell_size
      realized_arbitrage_size = min(investment, realized_buy_liquidity, realized_sell_liquidity)
      missed_opportunity = realized_arbitrage_size * (realized_sell_price/realized_buy_price - 1)

      logger.info(f'missed opportunity, $: {missed_opportunity: .2f}')
      total_missed_opportunity += missed_opportunity

  logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
  logger_ = logging.getLogger(__name__)
  logger_.setLevel(logging.INFO)

  logger_.info(f'Cycle ended. Results:')
  logger_.info(f'total number of trades: {number_of_bHsB + number_of_bBsH}')
  logger_.info(f'total expected profit, $: {total_expected_profit: .2f}')
  logger_.info(f'total realized profit, $: {total_realized_profit: .2f}')
  logger_.info(f'total missed opportunity, $: {total_missed_opportunity: .2f}')


  return total_expected_profit, total_realized_profit, total_missed_opportunity
