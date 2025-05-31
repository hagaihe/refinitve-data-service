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
        self._last_updated = datetime.now().date()
        self._load_today_cache()

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = ClosingPriceCache()
        return cls._instance

    def _is_cache_expired(self):
        return datetime.now().date() > self._last_updated

    def _load_today_cache(self):
        logging.info(f"Loading cache from {self._csv_path}")
        if os.path.exists(self._csv_path):
            with open(self._csv_path, mode='r', newline='') as file:
                reader = csv.DictReader(file)
                row_count = 0
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
            self._cache[symbol]['refinitiv_close'] = close_price
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
            file_exists = os.path.exists(self._csv_path)
            with open(self._csv_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                if not file_exists:
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
