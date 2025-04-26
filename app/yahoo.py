import asyncio
import logging
import time
from datetime import datetime, timedelta
from io import StringIO

import aiohttp
import pandas as pd
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


def get_epoch_time(date):
    return int(time.mktime(date.timetuple()))


def get_crumb_and_cookie_selenium(symbol):
    url = f"https://finance.yahoo.com/quote/{symbol}"

    # Set up Chrome options to force English language
    chrome_options = Options()
    chrome_options.add_argument("--lang=en-US")

    # Setup WebDriver with Chrome options
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # Visit the Yahoo Finance page
    driver.get(url)

    # Check if there's a consent form and accept it
    try:
        consent_button = driver.find_element(By.XPATH, '//button[text()="Accept"]')
        consent_button.click()
    except Exception as e:
        print(f"No consent form found: {e}")

    # Extract cookies
    cookies = driver.get_cookies()
    cookie_string = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])

    # Attempt to extract crumb
    try:
        crumb = driver.execute_script("return CrumbStore && CrumbStore.crumb;")
        if not crumb:
            raise Exception("CrumbStore not defined or no crumb found.")
    except Exception as e:
        print(f"Error fetching crumb: {e}")
        crumb = None

    driver.quit()

    return crumb, cookie_string


async def download_ex_div_data(symbol, semaphore):
    async with semaphore:
        # define the date range
        today = datetime.now().date()
        period1 = today - timedelta(days=60)
        period2 = today + timedelta(days=30)

        # convert dates to epoch times
        period1_epoch = get_epoch_time(period1)
        period2_epoch = get_epoch_time(period2)

        # Retrieve crumb and cookie
        crumb, cookie = await get_crumb_and_cookie_selenium(symbol)

        # Construct the URL with crumb
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?" \
              f"period1={period1_epoch}&period2={period2_epoch}&interval=1d&events=div&crumb={crumb}"

        headers = {
            "Cookie": cookie,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        try:
            logger.debug(f"call {url}...")
            async with aiohttp.ClientSession(headers=headers) as session:
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
        specific_date = pd.Timestamp(datetime.today().date() - timedelta(days=0)).strftime('%Y-%m-%d')

    logging.info(
        f"Request corporate actions data from yfinance on {len(input_universe)} symbols for date {specific_date}")

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
