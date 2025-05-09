from app.ib.ib_price_fetcher import IBPriceFetcher
from app.ib.ibclient import IBClient


async def fetch_last_adj_price(symbols: list[str], host: str, port: int) -> dict:
    ib_client = IBClient(host, port)
    fetcher = IBPriceFetcher(ib_client)
    return await fetcher.fetch_prices(symbols)