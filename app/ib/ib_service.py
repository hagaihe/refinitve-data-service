from app.ib.ib_price_fetcher import IBPriceFetcher
from app.ib.ibclient import IBClient


async def fetch_last_adj_price(symbols: list[str]) -> dict:
    async with IBClient() as ib_client:
        fetcher = IBPriceFetcher(ib_client)
        return await fetcher.fetch_prices(symbols)