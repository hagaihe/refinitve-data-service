import logging
import os
from datetime import datetime

import aiojobs as aiojobs
import refinitiv.data as rd
from aiohttp import web
from aiojobs.aiohttp import setup

from app.config import APP
from app.handlers import validate_corporate_actions_handler, health_check, get_holdings


def exception_handler(scheduler: aiojobs.Scheduler, context: dict):
    exception = context.get('exception', None)
    if exception is not None:
        logging.error("exception handler", exc_info=exception)


async def on_startup(app: web.Application):
    logging.info("setting up application")

    root_directory = os.path.dirname(os.path.abspath(__file__))
    log_directory = os.path.join(root_directory, 'logs')
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    current_date = datetime.now().strftime('%Y-%m-%d')
    log_file_path = os.path.join(log_directory, f"refinitiv-data-lib-{current_date}.log")
    logging.info(f"set refinitiv log={log_file_path}")

    APP.refinitive_config = rd.get_config()
    APP.refinitive_config.set_param("logs.transports.file.enabled", True)
    APP.refinitive_config.set_param("logs.transports.file.name", log_file_path)
    APP.refinitive_config.set_param("logs.level", "debug")


def application_init():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s')
    logging.getLogger('httpcore.http11').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore.connection').setLevel(logging.WARNING)
    logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    logging.info("init refinitive-data-service")
    webapp = web.Application(client_max_size=1024 ** 2 * 50)  # Set limit to 50 MB
    webapp.router.add_get('/health_check', health_check)
    webapp.router.add_get('/refinitive/holdings', get_holdings)
    webapp.router.add_post('/refinitive/corporate/validate', validate_corporate_actions_handler)
    setup(webapp, exception_handler=exception_handler, pending_limit=100)
    webapp.on_startup.append(on_startup)
    return webapp


if __name__ == '__main__':
    web.run_app(application_init(), port=8080)
