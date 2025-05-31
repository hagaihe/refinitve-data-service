import asyncio
import csv
import os
import logging
from datetime import datetime


class ClosingPriceCache:
    _instance = None
    _lock = asyncio.Lock()
    _csv_path = os.path.join(os.path.dirname(__file__), "storage", "closing_prices_log.csv")

    def __init__(self):
        logging.info("Initializing ClosingPriceCache...")
        self._cache = {}  # { "symbol": { "ib_close": float, "refinitiv_close": float, "date": str } }
        self._cache_lock = asyncio.Lock()
        if self._load_today_cache():
            first_symbol, first_record = next(iter(self._cache.items()))
            self._last_updated = datetime.strptime(first_record["date"], "%Y-%m-%d").date()
            logging.info(f"Cache last updated on {self._last_updated}")
        else:
            self._last_updated = datetime.now().date()

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = ClosingPriceCache()
        return cls._instance

    def _is_cache_expired(self):
        return datetime.now().date() > self._last_updated

    def _load_today_cache(self) -> bool:
        logging.info(f"Loading cache from {self._csv_path}")
        row_count = 0

        if os.path.exists(self._csv_path):
            with open(self._csv_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    symbol = row['symbol']
                    entry = {
                        'ib_close': float(row['ib_close']) if row['ib_close'] else None,
                        'refinitiv_close': float(row['refinitiv_close']) if row['refinitiv_close'] else None,
                        'date': row['date'],
                    }
                    self._cache[symbol] = entry
                    row_count += 1
                logging.info(f"Loaded {row_count} entries into cache")
        else:
            logging.info(f"Cache is not yet created")

        return row_count > 0

    async def _reset_if_expired(self):
        if self._is_cache_expired():
            async with self._cache_lock:
                logging.warning("Cache expired. Reloading from disk...")
                self._cache.clear()
                self._last_updated = datetime.now().date()
                self._load_today_cache()

    async def set_refinitiv_close(self, symbol: str, close_price: float, date: str):
        await self._reset_if_expired()
        async with self._cache_lock:
            if symbol not in self._cache:
                self._cache[symbol] = {'date': date}
            self._cache[symbol]['refinitiv_close'] = close_price if close_price != '<NA>' else None
            self._cache[symbol]['date'] = date
            logging.debug(f"Set Refinitiv close for {symbol}")
            await self._maybe_log_to_csv(symbol)

    async def set_ib_close(self, symbol: str, close_price: float, date: str):
        await self._reset_if_expired()
        async with self._cache_lock:
            if symbol not in self._cache:
                self._cache[symbol] = {'date': date}
            self._cache[symbol]['ib_close'] = close_price
            self._cache[symbol]['date'] = date
            logging.debug(f"Set IB close for {symbol}")
            await self._maybe_log_to_csv(symbol)

    async def _maybe_log_to_csv(self, symbol: str):
        data = self._cache.get(symbol, {})
        if all(k in data for k in ('refinitiv_close', 'ib_close', 'date')):
            row = [data['date'], symbol, data['ib_close'], data['refinitiv_close']]

            needs_header = False
            dir_path = os.path.dirname(self._csv_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                needs_header = True

            if (not os.path.exists(self._csv_path)) or (os.path.getsize(self._csv_path) == 0):
                needs_header = True

            with open(self._csv_path, mode="a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                if needs_header:
                    writer.writerow(['date', 'symbol', 'ib_close', 'refinitiv_close'])
                writer.writerow(row)
                logging.info(f"Logged prices for {symbol} on {data['date']}")

    async def get_prices(self, symbol: str):
        await self._reset_if_expired()
        async with self._cache_lock:
            return self._cache.get(symbol, None)

    async def fetch(self, symbol: str):
        await self._reset_if_expired()
        async with self._cache_lock:
            return self._cache.get(symbol, None)

    async def get_all(self):
        await self._reset_if_expired()
        async with self._cache_lock:
            return dict(self._cache)
