import random
from ib_insync import IB, Contract
from typing import Optional
import logging

from app.config import APP

logger = logging.getLogger(__name__)


class IBClient:
    def __init__(self, host: str = None, port: int = None,  client_id=1):
        self.host = host if host else APP.conf.ib_host
        self.port = port if port else APP.conf.ib_port
        self.client_id = (
            random.randint(1000, 999999) if client_id == 1 else client_id
        )
        self.ib: Optional[IB] = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def connect(self):
        try:
            if self.ib and self.ib.isConnected():
                return
            self.ib = IB()
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id, timeout=10)
            if not self.ib.isConnected():
                raise ConnectionError("Failed to connect to IB")
            logger.info("Connected to IB TWS")
        except Exception as e:
            logger.exception("Failed to connect to IB")
            raise e

    async def disconnect(self):
        try:
            if self.ib and self.ib.isConnected():
                self.ib.disconnect()
                logger.info("Disconnected from IB")
        except Exception as e:
            logger.warning(f"Error while disconnecting IB: {e}")

    async def resolve_contract(self, symbol: str) -> Optional[Contract]:
        base = Contract(symbol=symbol, secType='STK', exchange='SMART', currency='USD')
        details = await self.ib.reqContractDetailsAsync(base)
        if details:
            contract = details[0].contract
            logger.info(f"Resolved {symbol} to conId: {contract.conId}")
            return contract
        else:
            logger.warning(f"Could not resolve contract for symbol: {symbol}")
            return None

    async def fetch_adjusted_close(self, symbol: str) -> Optional[float]:
        contract = await self.resolve_contract(symbol)
        if not contract:
            logger.warning(f"Could not resolve contract for symbol: {symbol}")
            return None

        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime='',
            durationStr='1 D',
            barSizeSetting='1 day',
            whatToShow='ADJUSTED_LAST',
            useRTH=True,
            formatDate=1
        )
        if bars:
            return bars[-1].close
        return None
