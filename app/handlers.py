import json
from datetime import datetime

from aiohttp import web

from .utils import validate_corporate_actions


async def validate_corporate_actions_handler(request: web.Request):
    try:
        body = await request.json()
        symbols = body.get('symbols', [])
        if not symbols:
            raise ValueError("unable to validate corporate actions without symbols")

        fields = body.get('fields', ['ADJUST_CLS', 'HST_CLOSE', 'CAPITAL CHANGE EX DATE'])
        data, no_data_symbols, no_ric_symbols = await validate_corporate_actions(symbols, fields)

        return web.json_response({
            'corporate_actions': data,
            'no_data_symbols': no_data_symbols,
            'no_ric_symbols': no_ric_symbols
        })
    except Exception as e:
        # Remove escaped double quotes
        error_message = str(e).replace('"', '')
        return web.json_response({'error': error_message}, status=500)


# async def get_data_using_http(request: web.Request):
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
#     # Get authentication token
#     auth_response = requests.post(auth_url, data={
#         'username': username,
#         'password': password,
#         'grant_type': 'password',
#         'takeExclusiveSignOnControl': 'true',
#         'scope': 'trapi'
#     }, headers={
#         'Authorization': f'Basic {auth_header}',
#         'Content-Type': 'application/x-www-form-urlencoded'
#     })
#
#     # Check if authentication was successful
#     if auth_response.status_code != 200:
#         print(f"Authentication failed: {auth_response.text}")
#         exit()
#     else:
#         print(f"Authentication success!")
#
#     auth_data = auth_response.json()
#     access_token = auth_data['access_token']
#
#     # Specify the RIC and fields
#     ric = 'AAPL.O'  # Replace with the actual RIC
#     fields = ['TR.CAExDate']
#
#     # Request data
#     data_url = 'https://selectapi.datascope.refinitiv.com/restapi/v1/'
#
#     # Request data using POST
#     payload = {
#         "universe": [ric],
#         "fields": fields
#     }
#     header_req = {
#         'Authorization': f'Bearer {access_token}',
#         'Content-Type': 'application/json'
#     }
#     data_response = requests.post(data_url, headers=header_req, json=payload)
#     if data_response.status_code != 200:
#         print(f"Data request failed: {data_response.text}")
#
#     data = data_response.json()
#     print(json.dumps(data, indent=2))

def health_check(request: web.Request):
    message = {
        'utc-time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'health-check': 'healthy'
    }
    serialized = json.dumps(message, default=str)
    response = web.json_response(serialized)
    return response
