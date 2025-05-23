import json
import logging
from datetime import datetime

import pandas as pd
from aiohttp import web

from app.cache.closing_prices_cache import ClosingPriceCache
from app.config import APP
from app.ib.ib_service import fetch_last_adj_price
from app.refinitiv.refinitiv import fetch_holdings_for_symbol
from app.refinitiv.refinitive_service import fetch_corporate_actions


async def fetch_ib_last_adj_price_handler(request):
    try:
        data = await request.json()
        symbols = data.get('symbols', [])
        if not symbols:
            return web.json_response({'error': 'No symbols provided'}, status=400)

        res = await fetch_last_adj_price(APP.conf.ib_host, APP.conf.ib_port)
        return web.json_response(res)
    except Exception as e:
        logging.exception("Unhandled error in fetch_ib_last_adj_price_handler")
        return web.json_response({'error': str(e)}, status=500)


async def fetch_refinitiv_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("Unable to validate corporate actions without symbols")

        response = await fetch_corporate_actions(symbols)
        return web.json_response(response)

    except Exception as e:
        logging.exception("Unhandled error in fetch_refinitiv_corporate_actions_handler")
        return web.json_response({'error': str(e)}, status=500)


async def filter_daily_corporate_action_handler(request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            return web.json_response({'error': 'Missing ?symbols='}, status=400)

        # Run both fetchers concurrently
        # ib_task = asyncio.create_task(fetch_last_adj_price(symbols))
        # refinitiv_task = asyncio.create_task(fetch_corporate_actions(symbols))
        # ib_results, refinitiv_results = await asyncio.gather(ib_task, refinitiv_task)
        ib_results = await fetch_last_adj_price(symbols)
        refinitiv_results = await fetch_corporate_actions(symbols)

        if not ib_results.get("success"):
            return web.json_response(ib_results, status=500)

        flagged_set = set(ib_results.get("fetch_failed", [])) | set(refinitiv_results.get("flagged_symbols", []))

        cache = ClosingPriceCache.instance()
        for symbol in symbols:
            data = await cache.get_prices(symbol)
            ib_close = data.get("ib_close") if data else None
            ref_close = data.get("refinitiv_close") if data else None
            if ib_close is None or ref_close is None or \
                pd.isna(ib_close) or pd.isna(ref_close) or \
                abs(ib_close - ref_close) > 0.05:
                logging.warning(f"{symbol} is flagged: ib_close={ib_close} vs. refinitiv_close={ref_close}")
                flagged_set.add(symbol)

        # TODO: make this generic function
        def normalize_corporate_actions(data):
            for entry in data:
                for key, value in entry.items():
                    if isinstance(value, pd.Timestamp):
                        entry[key] = value.isoformat()
            return data

        normalized_actions = normalize_corporate_actions(refinitiv_results.get('corporate_actions', []))

        return web.json_response({
            'flagged_symbols': sorted(flagged_set),
            'corporate_actions': normalized_actions
        })
    except Exception as e:
        logging.exception("Unhandled error in filter_daily_corporate_action_handler")
        return web.json_response({'error': str(e)}, status=500)


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
        'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'last_trading_day': APP.conf.last_trading_day.strftime('%Y-%m-%d'),
        'health_check': 'healthy'
    }
    serialized = json.dumps(message, default=str)
    response = web.json_response(serialized)
    return response
