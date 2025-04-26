import asyncio
import logging
import os
import time
from datetime import datetime

import pandas as pd
import refinitiv.data as rd
from refinitiv.data._errors import RDError
from refinitiv.data.content import symbol_conversion

from app.cache.closing_prices_cache import ClosingPriceCache

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
