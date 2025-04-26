import asyncio


class ClosingPriceCache:
    _instance = None
    _lock = asyncio.Lock()

    def __init__(self):
        self._cache = {}  # { "symbol": { "refinitiv_close": float, "ib_close": float } }
        self._cache_lock = asyncio.Lock()  # Protects the actual cache operations

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = ClosingPriceCache()
        return cls._instance

    async def set_refinitiv_close(self, symbol: str, close_price: float):
        async with self._cache_lock:
            if symbol not in self._cache:
                self._cache[symbol] = {}
            self._cache[symbol]['refinitiv_close'] = close_price

    async def set_ib_close(self, symbol: str, close_price: float):
        async with self._cache_lock:
            if symbol not in self._cache:
                self._cache[symbol] = {}
            self._cache[symbol]['ib_close'] = close_price

    async def get_prices(self, symbol: str):
        async with self._cache_lock:
            return self._cache.get(symbol, None)

    async def fetch(self, symbol: str):
        async with self._cache_lock:
            return self._cache.get(symbol, None)

    async def get_all(self):
        async with self._cache_lock:
            return dict(self._cache)  # Shallow copy for safe reads
