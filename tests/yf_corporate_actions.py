import asyncio
import logging
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


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


async def validate_corporate_actions(input_universe, concurrent_requests_limit=5, specific_date=None):
    no_data_symbols = []

    if specific_date:
        today = specific_date
    else:
        today = pd.Timestamp(datetime.today().date()).strftime('%Y-%m-%d')

    logging.info(f"Request corporate actions data from yfinance on {len(input_universe)} symbols for date {today}")

    semaphore = asyncio.Semaphore(concurrent_requests_limit)

    async def fetch_data(symbol):
        try:
            data_df = await download_corporate_actions(symbol, semaphore)
            if not data_df.empty:
                return data_df[data_df['Date'] == today]
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
    else:
        results_df = pd.DataFrame()

    if not results_df.empty:
        logging.info(f"Found corporate actions\n{results_df}")

    logging.info("Request corporate actions data from yfinance ended")
    return results_df.to_dict(orient='records'), no_data_symbols, []


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    symbols = ["CACG"]
    specific_date = "2024-06-10"

    loop = asyncio.get_event_loop()
    results, no_data_symbols, _ = loop.run_until_complete(
        validate_corporate_actions(symbols, specific_date=specific_date))
    print("Results:", results)
    print("No data symbols:", no_data_symbols)
