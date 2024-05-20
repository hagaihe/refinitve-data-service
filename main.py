import logging
from aiohttp import web
from app.config import setup_config
from app.handlers import validate_corporate_actions_handler


def create_app():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info("init refinitive-data-service")
    setup_config()
    app = web.Application()
    logging.info("adding endpoints")
    app.router.add_post('/refinitive/corporate/validate', validate_corporate_actions_handler)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), port=8080)
