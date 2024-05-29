import dataclasses
import os


class AppConfig:
        def __init__(self):
                self.refinitiv_app_key = os.getenv('REFINITIV_APP_KEY')
                self.refinitiv_username = os.getenv('REFINITIV_USERNAME')
                self.refinitiv_password = os.getenv('REFINITIV_PASSWORD')

                if not self.refinitiv_app_key or not self.refinitiv_username or not self.refinitiv_password:
                    raise ValueError("Environment variables for Refinitiv credentials are not set properly.")


@dataclasses.dataclass
class App:
    conf: AppConfig = None
    refinitive_config = None


APP = App(conf=AppConfig())


