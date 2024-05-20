import logging
import refinitiv.data as rd
import pandas as pd

from app.config import REFINITIV_APP_KEY, REFINITIV_USERNAME, REFINITIV_PASSWORD

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


async def validate_corporate_actions(input_universe, input_fields):
    config = rd.get_config()
    config.set_param("logs.transports.file.enabled", True)
    config.set_param("logs.transports.file.name", "refinitiv-data-lib.log")
    config.set_param("logs.level", "debug")
    session = rd.session.platform.Definition(
        app_key=REFINITIV_APP_KEY,
        signon_control=True,
        grant=rd.session.platform.GrantPassword(
            username=REFINITIV_USERNAME,
            password=REFINITIV_PASSWORD
        )
    ).get_session()
    session.open()
    rd.session.set_default(session)

    result = []
    no_data_symbols = []
    try:
        pd.set_option('future.no_silent_downcasting', True)

        logging.info(f"requesting {input_fields} for {len(input_universe)}")
        data_df = rd.get_data(
            universe=input_universe,
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
            logging.info(f"found corporate actions on {filtered_df}")
        result = filtered_df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Failed to get data: {e}")
    finally:
        rd.close_session()
    return result, no_data_symbols
