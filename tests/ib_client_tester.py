import asyncio
import json
import logging
import os
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
        self.ib_client = IBClient(port=7481, client_id=158)
        self.fetcher = IBPriceFetcher(
            ib_client=self.ib_client,
            max_concurrent_requests=20,
            max_retries=2,
            batch_size=100,
            jitter_range_ms=(25, 100)
        )
        self.status_map = {}

    async def run(self):
        logger.info(f"Fetching prices for {len(test_symbols)} symbols")
        await self.fetcher.fetch_prices(test_symbols)

        cache = ClosingPriceCache.instance()
        status_map = self.fetcher.status_map

        fetched = []
        resolution_failed = []
        fetch_failed = []
        unknown = []

        for symbol in test_symbols:
            status = status_map.get(symbol)
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
        logger.info(f"Total symbols: {len(test_symbols)}")
        logger.info(f"Fetched: {len(fetched)}")
        logger.info(f"Resolution failed: {len(resolution_failed)}")
        logger.info(f"Fetch failed: {len(fetch_failed)}")
        logger.info(f"Unknown: {len(unknown)}")

        # Write summary JSON
        summary = {
            "total_requested": len(test_symbols),
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
    asyncio.run(tester.run())