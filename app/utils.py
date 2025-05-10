import logging
import os
import time
from datetime import datetime, time, timedelta
from pytz import timezone
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

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
    Return the previous trading day (excluding weekends and US federal holidays).
    """
    eastern = timezone('US/Eastern')
    if reference_date is None:
        reference_date = datetime.now(eastern).date()
    else:
        reference_date = pd.to_datetime(reference_date).date()

    previous_day = reference_date - timedelta(days=1)
    while previous_day.weekday() >= 5:  # Saturday = 5, Sunday = 6
        previous_day -= timedelta(days=1)

    holidays = USFederalHolidayCalendar().holidays(start="2000-01-01", end="2100-01-01")
    while pd.Timestamp(previous_day) in holidays:
        previous_day -= timedelta(days=1)

    return previous_day