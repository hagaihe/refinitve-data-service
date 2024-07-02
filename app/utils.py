import logging
from datetime import datetime

import pandas as pd
import refinitiv.data as rd
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

async def get_data(input_universe, input_fields):
    conf = APP.conf
    session = rd.session.platform.Definition(
        app_key=conf.refinitiv_app_key,
        signon_control=True,
        grant=rd.session.platform.GrantPassword(
            username=conf.refinitiv_username,
            password=conf.refinitiv_password
        )
    ).get_session()
    session.open()
    rd.session.set_default(session)

    logging.info(f"convert {len(input_universe)} symbols to rics")
    converted_symbols_dict = convert_to_ric(input_universe)
    rics = [s for s in converted_symbols_dict.values() if s is not None]

    no_ric_symbols = [k for k in converted_symbols_dict if converted_symbols_dict[k] is None]
    if no_ric_symbols:
        logging.error(f"the following symbols had no ric symbol={no_ric_symbols}")

    try:
        pd.set_option('future.no_silent_downcasting', True)
        logging.info(f"requesting {input_fields} for {rics}")

        data_df = rd.get_data(
            universe=rics,
            fields=input_fields
        )
        data_df = data_df.infer_objects(copy=False)
        logging.info(f"response: columns={data_df.columns.tolist()}, data count={len(data_df)}")

        # replace RICs in no_data_symbols with their corresponding requested symbols
        ric_to_symbol = {v: k for k, v in converted_symbols_dict.items()}
        data_df['Instrument'] = data_df['Instrument'].map(ric_to_symbol).fillna(data_df['Instrument'])

        return data_df, no_ric_symbols

    except Exception as e:
        logger.exception(f"Failed to get data")
        raise e


async def validate_corporate_actions(input_universe, input_fields):
    try:
        no_ric_symbols, data_df = await get_data(input_universe, input_fields)

        # Identify symbols with missing data in ADJUST_CLS or HST_CLOSE
        missing_data_df = data_df[data_df[[input_fields[0], input_fields[1]]].isna().any(axis=1)]
        no_data_symbols = missing_data_df['Instrument'].tolist()
        if no_data_symbols:
             logging.error(f"the following symbols had no data={no_data_symbols}")

        today = pd.Timestamp(datetime.today().date())

        # valid_data_df = data_df.dropna(subset=['ADJUST_CLS', 'HST_CLOSE'])
        # filtered_df = valid_data_df[
        #     (valid_data_df['ADJUST_CLS'] != valid_data_df['HST_CLOSE']) |
        #     (valid_data_df['CAPITAL CHANGE EX DATE'].notna() & (valid_data_df['CAPITAL CHANGE EX DATE'] == today))]

        filtered_df = data_df[
            (data_df['Dividend Ex Date'].notna() & (data_df['Dividend Ex Date'] == today))]

        if not filtered_df.empty:
            logging.info(f"found corporate actions\n{filtered_df}")

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
