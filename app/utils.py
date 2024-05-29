import logging
import refinitiv.data as rd
from refinitiv.data.content import symbol_conversion
import pandas as pd

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


def convert_to_ric(symbols):
    try:
        conversion_definition = symbol_conversion.Definition(
            symbols=symbols,
            from_symbol_type=symbol_conversion.SymbolTypes.TICKER_SYMBOL,
            to_symbol_types=[symbol_conversion.SymbolTypes.RIC],
            preferred_country_code=symbol_conversion.CountryCode.USA
        ).get_data()

        # Extracting the converted RICs
        converted_ric_list = []
        for symbol in symbols:
            try:
                ric = conversion_definition.data.raw['Matches'][symbol]['RIC']
                converted_ric_list.append(ric)
            except KeyError:
                logging.warning(f"No RIC found for symbol '{symbol}'")
                converted_ric_list.append(None)

        return converted_ric_list

    except Exception as e:
        logging.error(f"Error in converting symbols: {e}")
        return []

async def validate_corporate_actions(input_universe, input_fields):
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

    rics = convert_to_ric(input_universe)

    result = []
    no_data_symbols = []
    try:
        pd.set_option('future.no_silent_downcasting', True)

        logging.info(f"requesting {input_fields} for {len(rics)} symbols")
        data_df = rd.get_data(
            universe=rics,
            fields=input_fields
        )
        data_df = data_df.infer_objects(copy=False)
        logging.info(f"response from refinitive api contains data for {len(data_df)}")

        # Identify symbols with missing data in ADJUST_CLS or HST_CLOSE
        missing_data_df = data_df[data_df[['ADJUST_CLS', 'HST_CLOSE']].isna().any(axis=1)]
        no_data_symbols = missing_data_df['Instrument'].tolist()
        if no_data_symbols:
            logging.error(f"the following symbols had no data={no_data_symbols}")

        # Filter rows where ADJUST_CLS and HST_CLOSE are both not NaN
        valid_data_df = data_df.dropna(subset=['ADJUST_CLS', 'HST_CLOSE'])
        filtered_df = valid_data_df[valid_data_df['ADJUST_CLS'] != valid_data_df['HST_CLOSE']]
        if not filtered_df.empty:
            logging.info(f"found corporate actions\n{filtered_df}")
        result = filtered_df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to get data: {e}")
    finally:
        rd.close_session()
    return result, no_data_symbols
