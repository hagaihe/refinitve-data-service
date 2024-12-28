import logging
import pandas as pd
import refinitiv.data as rd
from app.utils import get_data


logger = logging.getLogger(__name__)


async def fetch_holdings_for_symbol(symbol):
    try:
        # Define the input universe (in this case, just the QQQ symbol)
        input_universe = [symbol]

        # Define the fields to fetch (replace these with the actual fields for holdings)
        # input_fields = ['holdings']
        input_fields = ['TR.Holdings']

        # Fetch the data using the existing get_data function
        data_df, no_ric_symbols = await get_data(input_universe, input_fields)

        # Handle symbols with missing RICs
        if no_ric_symbols:
            logging.error(f"the following symbols had no RIC symbol={no_ric_symbols}")

        # Filter or process the data as necessary
        if not data_df.empty:
            logging.info(f"Fetched holdings for symbol={symbol}\n{data_df}")

            # Convert DataFrame to dict for further processing or JSON serialization
            result = data_df.to_dict(orient='records')

            # Convert NaT to None for JSON serialization
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
        else:
            logging.warning(f"No holdings data found for symbol={symbol}")
            result = []

    except Exception as e:
        logger.exception(f"Failed to fetch holdings data")
        raise e
    finally:
        rd.close_session()

    return result, no_ric_symbols
