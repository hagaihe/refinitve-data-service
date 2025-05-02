
from app.cache.closing_prices_cache import ClosingPriceCache
from app.ib.ibclient import IBClient
import asyncio
import logging
from typing import List


logger = logging.getLogger(__name__)


class IBPriceFetcher:
    def __init__(self, ib_client: IBClient, max_concurrent_requests: int = 20, max_retries: int = 2):
        self.ib_client = ib_client
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.cache = ClosingPriceCache.instance()
        self.max_retries = max_retries

    async def fetch_prices(self, symbols: List[str]):
        for attempt in range(1, self.max_retries + 2):  # +1 for initial attempt
            try:
                await self.ib_client.connect()
                await self._fetch_prices_internal(symbols)
                return
            except Exception as e:
                logger.error(f"Attempt {attempt} failed with error: {e}")
                if attempt <= self.max_retries:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise
            finally:
                await self.ib_client.disconnect()

    async def _fetch_prices_internal(self, symbols: List[str]):
        tasks = [self._throttled_fetch(symbol) for symbol in symbols]
        await asyncio.gather(*tasks)

    async def _throttled_fetch(self, symbol: str):
        async with self.semaphore:
            try:
                price = await self.ib_client.fetch_adjusted_close(symbol)
                if price is not None:
                    await self.cache.set_ib_close(symbol, price)
                    logger.info(f"Fetched adjusted close for {symbol}: {price}")
                else:
                    logger.warning(f"No adjusted close data for {symbol}")
            except Exception as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")