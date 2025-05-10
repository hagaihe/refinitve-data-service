import dataclasses
import os
from dotenv import load_dotenv
from app.utils import get_previous_trading_day

load_dotenv()


class AppConfig:
    def __init__(self):
        self.refinitiv_app_key = os.getenv('REFINITIV_APP_KEY')
        self.refinitiv_username = os.getenv('REFINITIV_USERNAME')
        self.refinitiv_password = os.getenv('REFINITIV_PASSWORD')

        if not self.refinitiv_app_key or not self.refinitiv_username or not self.refinitiv_password:
            raise ValueError("Environment variables for Refinitiv credentials are not set properly.")

        # IB fetcher config
        self.ib_max_concurrent_requests = int(os.getenv('IB_MAX_CONCURRENT_REQUESTS', 20))
        self.ib_max_retries = int(os.getenv('IB_MAX_RETRIES', 2))
        self.ib_batch_size = int(os.getenv('IB_BATCH_SIZE', 100))
        self.ib_jitter_range_ms = (
            int(os.getenv('IB_JITTER_MIN_MS', 100)),
            int(os.getenv('IB_JITTER_MAX_MS', 300))
        )
        self.ib_max_concurrent_batches = int(os.getenv('IB_MAX_CONCURRENT_BATCHES', 3))
        self.ib_host= os.getenv('IB_HOST', '127.0.0.1')
        self.ib_port = int(os.getenv('IB_PORT', 7497))

        self.last_trading_day = get_previous_trading_day()


@dataclasses.dataclass
class App:
    conf: AppConfig = None
    refinitive_config = None


APP = App(conf=AppConfig())
