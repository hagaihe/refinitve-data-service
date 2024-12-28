import json
import logging
from datetime import datetime
from aiohttp import web
from app.refinitiv import fetch_holdings_for_symbol
from app.utils import validate_corporate_actions, serialize_timestamps, save_df_to_csv


async def validate_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("unable to validate corporate actions without symbols")

        fields = ['TR.DivExDate(SDate=2022-01-01,EDate=2027-12-31)'
                    ,'TR.AdjmtFactorAdjustmentDate(SDate=2023-01-01,EDate=2027-12-31)'
                    ,'TR.CAEffectiveDate(SDate=2023-01-01,EDate=2027-12-31)'
                    ,'TR.CARecordDate(SDate=2023-01-01,EDate=2027-12-31)']
        data, no_data_symbols, no_ric_symbols = await validate_corporate_actions(symbols, fields)

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

# async def get_holdings(request: web.Request):
#     # Replace these variables with your actual credentials
#     username = os.getenv('REFINITIV_USERNAME')
#     password = os.getenv('REFINITIV_PASSWORD')
#     api_key = os.getenv('REFINITIV_APP_KEY')
#
#     # Authentication URL
#     auth_url = 'https://api.refinitiv.com/auth/oauth2/v1/token'
#
#     # Base64 encode the api_key for the Authorization header
#     auth_header = base64.b64encode(f"{api_key}:".encode('utf-8')).decode('utf-8')
#
#     async with aiohttp.ClientSession() as session:
#         # Get authentication token
#         async with session.post(auth_url, data={
#             'username': username,
#             'password': password,
#             'grant_type': 'password',
#             'takeExclusiveSignOnControl': 'true',
#             'scope': 'trapi'
#         }, headers={
#             'Authorization': f'Basic {auth_header}',
#             'Content-Type': 'application/x-www-form-urlencoded'
#         }) as auth_response:
#
#             if auth_response.status != 200:
#                 print(f"Authentication failed: {await auth_response.text()}")
#                 return
#             else:
#                 print(f"Authentication success!")
#
#             auth_data = await auth_response.json()
#             access_token = auth_data['access_token']
#
#             # Data request URL
#             data_url = 'https://api.refinitiv.com/data/funds/v1/assets?symbols=LP40065886&properties=holdings[start:2024-03-31;end:2024-07-31]'
#
#             # Request data using GET
#             headers = {
#                 'Authorization': f'Bearer {access_token}',
#                 'Content-Type': 'application/json'
#             }
#
#             async with session.get(data_url, headers=headers) as data_response:
#                 if data_response.status != 200:
#                     print(f"Data request failed: {await data_response.text()}")
#                     return
#
#                 data = await data_response.json()
#                 print(json.dumps(data, indent=2))
#                 return data


def health_check(request: web.Request):
    message = {
        'utc-time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health-check': 'healthy'
    }
    serialized = json.dumps(message, default=str)
    response = web.json_response(serialized)
    return response
