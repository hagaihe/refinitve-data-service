import asyncio
import logging
import aiohttp
import time
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import yfinance as yf

logger = logging.getLogger(__name__)


def get_epoch_time(date):
    return int(time.mktime(date.timetuple()))


async def download_ex_div_data(symbol, semaphore):
    async with semaphore:
        # define the date range
        today = datetime.now().date()
        period1 = today - timedelta(days=60)
        period2 = today + timedelta(days=30)

        # convert dates to epoch times
        period1_epoch = get_epoch_time(period1)
        period2_epoch = get_epoch_time(period2)

        # construct the URL
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?" \
              f"period1={period1_epoch}&period2={period2_epoch}&interval=1d&events=div&includeAdjustedClose=true"

        try:
            logger.debug(f"call {url}...")
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        data_df = pd.read_csv(StringIO(content))
                        if not data_df.empty:
                            # sort data by 'Date' column in ascending order
                            logging.debug(f"ex-div for {symbol}:\n{data_df}")
                            data_df['Instrument'] = symbol
                            data_df['Date'] = pd.to_datetime(data_df['Date'])
                            data_df = data_df.sort_values(by='Date')
                            return data_df
                    else:
                        raise Exception(f"failed to download data for {symbol}. HTTP Status code: {response.status}")
        except Exception as ex:
            logger.exception("failed to fetch ex-div data from yahoo finance")
            raise ex

        return pd.DataFrame()


async def download_corporate_actions(symbol, semaphore):
    async with semaphore:
        try:
            logger.debug(f"Fetching data for {symbol}...")
            stock = yf.Ticker(symbol)
            actions = stock.actions

            if not actions.empty:
                actions['Instrument'] = symbol
                actions['Date'] = pd.to_datetime(actions.index).strftime('%Y-%m-%d')
                actions = actions.reset_index(drop=True)
                return actions
        except Exception as ex:
            logger.exception(f"Failed to fetch data for {symbol} using yfinance")
            raise ex

        return pd.DataFrame()


async def validate_corporate_actions_v1(input_universe, concurrent_requests_limit=1):
    t0 = time.time()
    no_data_symbols = []
    today = pd.Timestamp(datetime.today().date() - timedelta(days=0))
    logging.info(f"Request ex-div data from Yahoo on {len(input_universe)} symbols")

    semaphore = asyncio.Semaphore(concurrent_requests_limit)

    async def fetch_data(symbol):
        try:
            data_df = await download_ex_div_data(symbol, semaphore)
            if not data_df.empty:
                return data_df.iloc[-1]
        except Exception as e:
            logger.exception(f"Failed to get data for {symbol}")
            no_data_symbols.append(symbol)
        return None

    tasks = [fetch_data(symbol) for symbol in input_universe]
    data = await asyncio.gather(*tasks)

    results = [record for record in data if record is not None]

    if no_data_symbols:
        logging.error(f"The following symbols had no data: {no_data_symbols}")

    if results:
        results_df = pd.DataFrame(results).reset_index(drop=True)
        filtered_df = results_df.loc[(results_df['Date'].notna()) & (results_df['Date'] == today)]
        # filtered_df = results_df.loc[(results_df['Date'].notna())]
        # Convert the 'Date' column to string format for JSON serialization
        filtered_df['Date'] = filtered_df['Date'].dt.strftime('%Y-%m-%d')
    else:
        filtered_df = pd.DataFrame()

    if not filtered_df.empty:
        logging.info(f"Found corporate actions\n{filtered_df}")

    logging.info("Request ex-div data from Yahoo ended")

    elapsed = time.time() - t0
    logging.info(f"Runtime: {elapsed} seconds")

    return filtered_df.to_dict(orient='records'), no_data_symbols, []


async def validate_corporate_actions_v2(input_universe, concurrent_requests_limit=5, specific_date=None):
    t0 = time.time()
    no_data_symbols = []

    if not specific_date:
        specific_date = pd.Timestamp(datetime.today().date() - timedelta(days=1)).strftime('%Y-%m-%d')

    logging.info(f"Request corporate actions data from yfinance on {len(input_universe)} symbols for date {specific_date}")

    semaphore = asyncio.Semaphore(concurrent_requests_limit)

    async def fetch_data(symbol):
        try:
            data_df = await download_corporate_actions(symbol, semaphore)
            if not data_df.empty:
                return data_df[data_df['Date'] == specific_date]
        except Exception as e:
            logger.exception(f"Failed to get data for {symbol}")
            no_data_symbols.append(symbol)
        return None

    tasks = [fetch_data(symbol) for symbol in input_universe]
    data = await asyncio.gather(*tasks)

    results = [record for record in data if record is not None and not record.empty]

    if no_data_symbols:
        logging.error(f"The following symbols had no data: {no_data_symbols}")

    if results:
        results_df = pd.concat(results).reset_index(drop=True)
        logging.info(f"Found corporate actions\n{results_df}")
    else:
        results_df = pd.DataFrame()

    logging.info("Request corporate actions data from yfinance ended")

    elapsed = time.time() - t0
    logging.info(f"Runtime: {elapsed} seconds")
    return results_df.to_dict(orient='records'), no_data_symbols, []