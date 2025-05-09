import asyncio
import json
import logging
import os
import random
from datetime import datetime

from app.cache.closing_prices_cache import ClosingPriceCache
from app.ib.ib_price_fetcher import IBPriceFetcher
from app.ib.ibclient import IBClient
from tests.testing_symbols import test_symbols

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s')
logger = logging.getLogger(__name__)


class IBPriceFetcherTest:
    def __init__(self):
        self.client_id = random.randint(1000, 999999)
        self.status_map = {}

    async def run(self, symbols):
        logger.info(f"Fetching prices for {len(symbols)} symbols")

        async with IBClient(client_id=self.client_id) as ib_client:
            fetcher = IBPriceFetcher(ib_client=ib_client)
            await fetcher.fetch_prices(symbols)
            self.status_map = fetcher.status_map

        await self._report(symbols)

    async def _report(self, symbols):
        cache = ClosingPriceCache.instance()
        fetched = []
        resolution_failed = []
        fetch_failed = []
        unknown = []

        for symbol in symbols:
            status = self.status_map.get(symbol)
            if status == 'fetched':
                prices = await cache.get_prices(symbol)
                logger.info(f"✅ {symbol}: Adjusted close = {prices['ib_close']}")
                fetched.append(symbol)
            elif status == 'resolution_failed':
                logger.warning(f"⚠️ {symbol}: Contract resolution failed")
                resolution_failed.append(symbol)
            elif status == 'fetch_failed':
                logger.warning(f"❌ {symbol}: Price fetch failed")
                fetch_failed.append(symbol)
            else:
                logger.warning(f"❓ {symbol}: Unknown status")
                unknown.append(symbol)

        # Summary
        logger.info("=== Summary ===")
        logger.info(f"Total symbols: {len(symbols)}")
        logger.info(f"Fetched: {len(fetched)}")
        logger.info(f"Resolution failed: {len(resolution_failed)}")
        logger.info(f"Fetch failed: {len(fetch_failed)}")
        logger.info(f"Unknown: {len(unknown)}")

        summary = {
            "total_requested": len(symbols),
            "fetched": fetched,
            "resolution_failed": resolution_failed,
            "fetch_failed": fetch_failed,
            "unknown": unknown,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        logs_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        filename = f"ib_price_fetch_summary_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        summary_path = os.path.join(logs_dir, filename)

        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Summary JSON written to {summary_path}")


if __name__ == '__main__':
    tester = IBPriceFetcherTest()
    asyncio.run(tester.run(test_symbols))