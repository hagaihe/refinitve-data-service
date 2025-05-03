import asyncio
import logging
import random
from typing import List, Dict, Optional
from app.cache.closing_prices_cache import ClosingPriceCache
from app.ib.ibclient import IBClient

logger = logging.getLogger(__name__)


class IBPriceFetcher:
    def __init__(
        self,
        ib_client: IBClient,
        max_concurrent_requests: int = 20,
        max_retries: int = 2,
        batch_size: int = 100,
        jitter_range_ms: Optional[tuple] = (0, 100),
    ):
        self.ib_client = ib_client
        self.max_concurrent_requests = max_concurrent_requests
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.jitter_range_ms = jitter_range_ms
        self.cache = ClosingPriceCache.instance()
        self.status_map: Dict[str, str] = {}

    async def fetch_prices(self, symbols: List[str]):
        await self.ib_client.connect()
        try:
            status_map: Dict[str, str] = {}
            remaining_symbols = symbols.copy()

            for attempt in range(1, self.max_retries + 2):
                logger.info(f"Fetch attempt {attempt}, symbols left: {len(remaining_symbols)}")
                failed: List[str] = []

                for i in range(0, len(remaining_symbols), self.batch_size):
                    batch = remaining_symbols[i:i + self.batch_size]
                    batch_failed = await self._process_batch(batch, status_map)
                    failed.extend(batch_failed)

                if not failed:
                    logger.info("All symbols fetched or failed definitively.")
                    break
                elif attempt <= self.max_retries:
                    logger.warning(f"{len(failed)} symbols failed on attempt {attempt}, retrying...")
                    await asyncio.sleep(2 ** attempt)
                    remaining_symbols = failed
                else:
                    logger.error(f"Final attempt failed. Unresolved symbols: {failed}")
                    break

            # Report
            fetched = [s for s, status in status_map.items() if status == 'fetched']
            resolution_failed = [s for s, status in status_map.items() if status == 'resolution_failed']
            fetch_failed = [s for s, status in status_map.items() if status == 'fetch_failed']

            logger.info(f"Summary:")
            logger.info(f"  Total symbols: {len(symbols)}")
            logger.info(f"  Successfully fetched: {len(fetched)}")
            logger.info(f"  Resolution failed: {len(resolution_failed)}")
            logger.info(f"  Fetch failed (e.g., timeout, data error): {len(fetch_failed)}")

            self.status_map = status_map

        finally:
            await self.ib_client.disconnect()

    async def _process_batch(self, symbols: List[str], status_map: Dict[str, str]) -> List[str]:
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        tasks = [self._throttled_fetch(symbol, semaphore, status_map) for symbol in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return [s for s in symbols if status_map.get(s) not in ('fetched', 'resolution_failed')]

    async def _throttled_fetch(self, symbol: str, semaphore: asyncio.Semaphore, status_map: Dict[str, str]):
        async with semaphore:
            try:
                if self.jitter_range_ms:
                    jitter = random.randint(*self.jitter_range_ms)
                    await asyncio.sleep(jitter / 1000.0)

                price = await self.ib_client.fetch_adjusted_close(symbol)
                if price is not None:
                    await self.cache.set_ib_close(symbol, price)
                    logger.info(f"Fetched adjusted close for {symbol}: {price}")
                    status_map[symbol] = 'fetched'
                else:
                    raise ValueError("No price returned")
            except ValueError as ve:
                if "Could not resolve contract" in str(ve):
                    logger.warning(f"{symbol} contract resolution failed: {ve}")
                    status_map[symbol] = 'resolution_failed'
                else:
                    logger.error(f"{symbol} failed: {ve}")
                    status_map[symbol] = 'fetch_failed'
            except Exception as e:
                logger.error(f"{symbol} fetch exception: {e}")
                status_map[symbol] = 'fetch_failed'
