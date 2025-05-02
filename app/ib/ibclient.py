import asyncio
import sys

from ib_insync import IB, ContractDetails, Stock, Contract
from typing import Optional, Union
import logging

logger = logging.getLogger(__name__)


class IBClient:
    def __init__(self, host='127.0.0.1', port=7497, client_id=1):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib: Optional[IB] = None

    async def connect(self):
        try:
            if self.ib and self.ib.isConnected():
                return
            self.ib = IB()
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
            if not self.ib.isConnected():
                raise ConnectionError("Failed to connect to IB")
            logger.info("Connected to IB TWS")
        except Exception as e:
            logger.exception("Failed to connect to IB")
            raise e

    async def disconnect(self):
        if self.ib and self.ib.isConnected():
            self.ib.disconnect()
            logger.info("Disconnected from IB")

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
