import json
from datetime import datetime

from aiohttp import web
from .utils import convert_to_refinitiv_symbology, validate_corporate_actions


async def validate_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("unable to validate corporate actions without symbols")

        fields = body.get('fields', ['ADJUST_CLS', 'HST_CLOSE'])
        data, no_data_symbols, no_ric_symbols = await validate_corporate_actions(symbols, fields)

        return web.json_response({
            'corporate_actions': data,
            'no_data_symbols': no_data_symbols,
            'no_ric_symbols': no_ric_symbols
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


def health_check(request: web.Request):
    message = {
        'utc-time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health-check': 'healthy'
    }
    serialized = json.dumps(message, default=str)
    response = web.json_response(serialized)
    return response
