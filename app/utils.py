import logging
import os
import time
from datetime import datetime, time, timedelta
from pytz import timezone
import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


def batch_symbols(symbols, batch_size=50):
    for i in range(0, len(symbols), batch_size):
        yield symbols[i:i + batch_size]


def serialize_timestamps(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return obj


def save_df_to_csv(df, file_prefix='corporate_actions', folder='data_output'):
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'{file_prefix}_{timestamp}.csv'
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False)
    return filepath


def is_after_market_open():
    eastern = timezone('US/Eastern')
    now_et = datetime.now(eastern)
    market_open = time(9, 30)
    return now_et.time() >= market_open


def get_previous_trading_day(reference_date=None):
    """
    Return the previous trading day using the NYSE calendar.
    """
    eastern = timezone('US/Eastern')
    if reference_date is None:
        reference_date = datetime.now(eastern).date()
    else:
        reference_date = pd.to_datetime(reference_date).date()

    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date='2000-01-01', end_date=reference_date)
    trading_days = schedule.index

    # Find the last trading day before the reference date
    previous_days = trading_days[trading_days < pd.Timestamp(reference_date)]
    if not previous_days.empty:
        last_trading_day = previous_days[-1].date()
        return last_trading_day
    else:
        raise SystemExit("No trading day found before the reference date")