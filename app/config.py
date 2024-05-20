import logging
import os
import json

REFINITIV_APP_KEY = os.getenv('REFINITIV_APP_KEY')
REFINITIV_USERNAME = os.getenv('REFINITIV_USERNAME')
REFINITIV_PASSWORD = os.getenv('REFINITIV_PASSWORD')

def setup_config():

    if not REFINITIV_APP_KEY or not REFINITIV_USERNAME or not REFINITIV_PASSWORD:
        raise ValueError("Environment variables for Refinitiv credentials are not set properly.")

    config_file_name = 'refinitiv-data.config.json'

    config = {
            "sessions": {
                "default": "platform.rdp",
                "platform": {
                "rdp": {
                    "app-key": REFINITIV_APP_KEY,
                    "user_name": REFINITIV_USERNAME,
                    "password": REFINITIV_PASSWORD,
                    "auto-reconnect": True,
                    "server-mode": True,
                    "signon_control": True
                }
            }
        }
    }

    logging.info(f"load & update refinitive config to {config_file_name}")
    if os.path.exists(config_file_name):
        os.remove(config_file_name)

    with open(config_file_name, 'w') as config_file:
        json.dump(config, config_file, indent=4)
