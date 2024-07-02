import asyncio
import logging
import aiohttp
import time
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

logger = logging.getLogger(__name__)


def get_epoch_time(date):
    return int(time.mktime(date.timetuple()))


async def download_ex_div_data(symbol):
    # define the date range
    today = datetime.now().date()
    period1 = today - timedelta(days=7)
    period2 = today + timedelta(days=1)

    # convert dates to epoch times
    period1_epoch = get_epoch_time(period1)
    period2_epoch = get_epoch_time(period2)

    # construct the URL
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?" \
          f"period1={period1_epoch}&period2={period2_epoch}&interval=1d&events=div&includeAdjustedClose=true"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    data_df = pd.read_csv(StringIO(content))
                    if not data_df.empty:
                        # sort data by 'Date' column in ascending order
                        data_df['Instrument'] = symbol
                        data_df['Date'] = pd.to_datetime(data_df['Date'])
                        data_df = data_df.sort_values(by='Date')
                        return data_df
                else:
                    logger.info(f"failed to download data for {symbol}. HTTP Status code: {response.status}")
    except Exception as ex:
        logger.exception("failed to fetch ex-div data from yahoo finance")
        raise ex

    return pd.DataFrame()


async def validate_corporate_actions(input_universe):
    results = []
    no_data_symbols = []
    today = pd.Timestamp(datetime.today().date() - timedelta(days=1))
    logging.info(f"request for ex-div data from yahoo on {len(input_universe)} symbols")

    for symbol in input_universe:
        try:
            data_df = await download_ex_div_data(symbol)
            if not data_df.empty:
                results.append(data_df.iloc[-1])
        except Exception as e:
            logger.exception(f"Failed to get data")
            no_data_symbols.append(symbol)

    if no_data_symbols:
        logging.error(f"the following symbols had no data={no_data_symbols}")

    if results:
        results_df = pd.DataFrame(results).reset_index(drop=True)
        filtered_df = results_df[(results_df['Date'].notna()) & (results_df['Date'] == today)]
        # filtered_df = results_df
    else:
        filtered_df = pd.DataFrame()

    if not filtered_df.empty:
        logging.info(f"found corporate actions\n{filtered_df}")

    # convert the 'Date' column to string format for JSON serialization
    filtered_df['Date'] = filtered_df['Date'].dt.strftime('%Y-%m-%d')

    logging.info(f"request for ex-div data from yahoo ended")

    return filtered_df.to_dict(orient='records'), no_data_symbols, []
