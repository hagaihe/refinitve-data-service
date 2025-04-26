import asyncio
import json
import logging
from datetime import datetime, timedelta
import refinitiv.data as rd
from aiohttp import web
from app.config import APP
from app.refinitiv import fetch_holdings_for_symbol, refinitiv_corporate_actions, refinitiv_fetch_close_prices
from app.utils import batch_symbols


async def validate_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("Unable to validate corporate actions without symbols")

        today = datetime.utcnow().date()
        start_date = (today - timedelta(days=5*365)).strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=2*365)).strftime("%Y-%m-%d")

        fields = [
            f'TR.DivExDate(SDate={start_date},EDate={end_date})',
            f'TR.AdjmtFactorAdjustmentDate(SDate={start_date},EDate={end_date})',
            f'TR.CAEffectiveDate(SDate={start_date},EDate={end_date})',
            f'TR.CARecordDate(SDate={start_date},EDate={end_date})'
        ]

        logging.info(f"Fetching corporate actions for {len(symbols)} symbols from {start_date} to {end_date}...")

        # Connect to Refinitiv
        conf = APP.conf
        logging.info("Connecting to refiniv...")
        session = rd.session.platform.Definition(
            app_key=conf.refinitiv_app_key,
            signon_control=True,
            grant=rd.session.platform.GrantPassword(
                username=conf.refinitiv_username,
                password=conf.refinitiv_password
            )
        ).get_session()
        rd.get_config()["http.request-timeout"] = 300
        rd.get_config()["http.connect-timeout"] = 300
        session.open()
        rd.session.set_default(session)
        logging.info(f"Connected to refiniv. SessionId={session.open_state}, ServerMode={session.server_mode}")

        symbol_batches = list(batch_symbols(symbols, batch_size=30))
        async def fetch_batch(batch):
            try:
                data, no_data_symbols, no_ric_symbols = await refinitiv_corporate_actions(batch, fields)
                await refinitiv_fetch_close_prices(batch)
                return data, no_data_symbols, no_ric_symbols
            except Exception as e:
                logging.error(f"Error fetching batch {batch}: {e}")
                return {}, batch, []

        # Fetch all batches concurrently
        tasks = [fetch_batch(batch) for batch in symbol_batches]
        results = await asyncio.gather(*tasks)

        corporate_actions = {}
        flagged_symbols = []

        for data, no_data_symbols, no_ric_symbols in results:
            if data:
                corporate_actions.update(data)
            flagged_symbols.extend(no_data_symbols)
            flagged_symbols.extend(no_ric_symbols)

        # Remove duplicates
        flagged_symbols = list(set(flagged_symbols))
        logging.info(f"Completed fetching. {len(corporate_actions)} symbols with corporate actions, {len(flagged_symbols)} flagged.")

        response = {
            'corporate_actions': corporate_actions,
            'flagged_symbols': flagged_symbols
        }
        return web.json_response(response)

    except Exception as e:
        logging.exception("Unhandled error in validate_corporate_actions_handler")
        return web.json_response({'error': str(e)}, status=500)

    finally:
        logging.info("Closing Refinitiv session")
        rd.close_session()


async def get_holdings(request: web.Request):
    try:
        index = 'QQQ'
        holdings_data, no_ric_symbols = await fetch_holdings_for_symbol(index)
        return web.json_response({
            'index': index,
            'holdings_data': holdings_data,
        })
    except Exception as e:
        # Remove escaped double quotes
        error_message = str(e).replace('"', '')
        return web.json_response({'error': error_message}, status=404)


def health_check(request: web.Request):
    message = {
        'utc-time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health-check': 'healthy'
    }
    serialized = json.dumps(message, default=str)
    response = web.json_response(serialized)
    return response
