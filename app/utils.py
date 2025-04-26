import asyncio
import logging
import os
import time
from datetime import datetime

import pandas as pd
import refinitiv.data as rd
from refinitiv.data._errors import RDError
from refinitiv.data.content import symbol_conversion

from app.config import APP

logger = logging.getLogger(__name__)


def convert_to_refinitiv_symbology(symbols):
    converted = []
    ignored = []
    for symbol in symbols:
        if symbol.isalpha():
            symbol = symbol.upper()
            converted.append(f"{symbol}.O")
        else:
            logging.warning(f"Symbol '{symbol}' is not purely alphabetic and was ignored.")
            ignored.append(symbol)
    return converted, ignored


def convert_to_ric(symbols) -> dict:
    try:
        conversion_definition = symbol_conversion.Definition(
            symbols=symbols,
            from_symbol_type=symbol_conversion.SymbolTypes.TICKER_SYMBOL,
            to_symbol_types=[symbol_conversion.SymbolTypes.RIC],
            preferred_country_code=symbol_conversion.CountryCode.USA
        ).get_data()

        # Extracting the converted RICs
        converted_ric_list = {}
        for symbol in symbols:
            try:
                ric = conversion_definition.data.raw['Matches'][symbol]['RIC']
                converted_ric_list[symbol] = ric
            except KeyError:
                logging.warning(f"No RIC found for symbol '{symbol}'")
                converted_ric_list[symbol] = None

        return converted_ric_list

    except Exception as e:
        logging.error(f"Error in converting symbols: {e}")
        return []


def fetch_data_with_retry(rics, input_fields):
    """ Synchronous function to fetch data with retry handling. """
    import refinitiv.data as rd
    return rd.get_data(universe=rics, fields=input_fields)


async def get_data(input_universe, input_fields, retries=3):
    conf = APP.conf
    session = rd.session.platform.Definition(
        app_key=conf.refinitiv_app_key,
        signon_control=True,
        grant=rd.session.platform.GrantPassword(
            username=conf.refinitiv_username,
            password=conf.refinitiv_password
        )
    ).get_session()
    # Set global timeout configuration
    rd.get_config()["http.request-timeout"] = 300  # 5 minutes
    rd.get_config()["http.connect-timeout"] = 300  # Connect timeout
    session.open()
    rd.session.set_default(session)

    logging.info(f"convert {len(input_universe)} symbols to rics")
    converted_symbols_dict = convert_to_ric(input_universe)
    rics = [s for s in converted_symbols_dict.values() if s is not None]

    no_ric_symbols = [k for k in converted_symbols_dict if converted_symbols_dict[k] is None]
    if no_ric_symbols:
        logging.error(f"the following symbols had no ric symbol={no_ric_symbols}")

    attempt = 0
    while attempt < retries:
        try:
            logging.info(f"Attempt {attempt + 1}: requesting {input_fields} for {rics}")
            data_df = await asyncio.to_thread(rd.get_data, universe=rics, fields=input_fields)

            data_df = data_df.infer_objects(copy=False)
            logging.info(f"response: columns={data_df.columns.tolist()}, data count={len(data_df)}")

            # replace RICs in no_data_symbols with their corresponding requested symbols
            ric_to_symbol = {v: k for k, v in converted_symbols_dict.items()}
            data_df['Instrument'] = data_df['Instrument'].map(ric_to_symbol).fillna(data_df['Instrument'])

            return data_df, no_ric_symbols

        except (asyncio.TimeoutError, RDError) as e:
            logging.error(f"Error occurred during data retrieval: {str(e)}. Attempt {attempt + 1} failed.")
            attempt += 1
            time.sleep(2)
            continue

        except Exception as e:
            logging.exception(f"An unexpected error occurred during data retrieval: {str(e)}")
            raise e

    raise Exception(f"Failed to retrieve data after {retries} attempts")


async def refinitiv_corporate_actions(input_universe, input_fields):
    try:
        data_df, no_ric_symbols = await get_data(input_universe, input_fields)
        no_data_symbols = [symbol for symbol in input_universe if symbol not in data_df['Instrument'].values]

        # filter only rows with date future value
        today = pd.Timestamp(datetime.today().date())
        filtered_df = data_df[data_df.iloc[:, 1:].apply(lambda row: row.notna() & (row == today), axis=1).any(axis=1)]

        if not filtered_df.empty:
            logging.info(f"found {len(filtered_df)} corporate actions")
            logging.info(f"DataFrame saved to: {save_df_to_csv(filtered_df)}")

        result = filtered_df.to_dict(orient='records')

        # convert NaT to None for JSON serialization
        for record in result:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
    except Exception as e:
        logger.exception(f"Failed to get data")
        raise e
    finally:
        rd.close_session()
    return result, no_data_symbols, no_ric_symbols


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
