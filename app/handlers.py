import json
from datetime import datetime

from aiohttp import web

from app.refinitiv import fetch_holdings_for_symbol
from app.utils import refinitiv_corporate_actions


async def validate_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("unable to validate corporate actions without symbols")

        fields = ['TR.DivExDate(SDate=2022-01-01,EDate=2027-12-31)'
            , 'TR.AdjmtFactorAdjustmentDate(SDate=2023-01-01,EDate=2027-12-31)'
            , 'TR.CAEffectiveDate(SDate=2023-01-01,EDate=2027-12-31)'
            , 'TR.CARecordDate(SDate=2023-01-01,EDate=2027-12-31)']
        data, no_data_symbols, no_ric_symbols = await refinitiv_corporate_actions(symbols, fields)

        serializable_data = json.loads(json.dumps(data, default=str))
        return web.json_response({
            'corporate_actions': serializable_data,
            'no_data_symbols': no_data_symbols,
            'no_ric_symbols': no_ric_symbols
        })
    except Exception as e:
        # remove escaped double quotes
        error_message = str(e).replace('"', '')
        return web.json_response({'error': error_message}, status=404)


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
