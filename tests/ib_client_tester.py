import asyncio
import logging

from app.cache.closing_prices_cache import ClosingPriceCache
from app.ib.ib_price_fetcher import IBPriceFetcher
from app.ib.ibclient import IBClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s')
logger = logging.getLogger(__name__)


class IBPriceFetcherTest:
    def __init__(self):
        self.ib_client = IBClient(port=7481, client_id=158)
        self.fetcher = IBPriceFetcher(self.ib_client)

    async def run(self):
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
                        'META', 'NVDA', 'SPY', 'QQQ', 'VTI']

        await self.fetcher.fetch_prices(test_symbols)

        # Print cached prices
        cache = ClosingPriceCache.instance()
        for symbol in test_symbols:
            prices = await cache.get_prices(symbol)
            if prices and 'ib_close' in prices:
                logger.info(f"Cached {symbol} adjusted close: {prices['ib_close']}")
            else:
                logger.warning(f"{symbol} has no adjusted close in cache")


if __name__ == '__main__':
    tester = IBPriceFetcherTest()
    asyncio.run(tester.run())
