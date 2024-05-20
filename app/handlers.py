from aiohttp import web
from .utils import convert_to_refinitiv_symbology, validate_corporate_actions


async def validate_corporate_actions_handler(request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("unable to validate corporate actions without symbols")

        fields = body.get('fields', ['ADJUST_CLS', 'HST_CLOSE'])

        refinitiv_symbols, ignored = convert_to_refinitiv_symbology(symbols)
        data, no_data_symbols = await validate_corporate_actions(refinitiv_symbols, fields)

        return web.json_response({
            'data': data,
            'no_data_symbols': no_data_symbols
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)
