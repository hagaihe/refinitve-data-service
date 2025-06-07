import asyncio
import csv
import logging
import os
from datetime import datetime
from typing import Dict, Optional, List

from app.cache.contract_meta_data_schema import FIELDNAMES


class ContractMetadataCache:
    _instance = None
    _csv_path = os.path.join(os.path.dirname(__file__), "storage", "contract_metadata.csv")

    def __init__(self):
        self._cache: Dict[str, Dict[str, str]] = {}
        self._cache_lock = asyncio.Lock()
        self._initialize_csv()
        self._load_from_csv()

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _initialize_csv(self):
        if not os.path.exists(self._csv_path):
            os.makedirs(os.path.dirname(self._csv_path), exist_ok=True)
            with open(self._csv_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
                writer.writeheader()
            logging.info(f"Created metadata db in {self._csv_path}")

    def _load_from_csv(self):
        try:
            with open(self._csv_path, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    symbol = row['symbol']
                    self._cache[symbol] = row
            if self._cache:
                logging.info(f"Loaded {len(self._cache)} records into contract metadata cache")
        except Exception as e:
            logging.error(f"Error loading from CSV file: {e}")

    def _save_to_csv(self):
        try:
            with open(self._csv_path, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
                writer.writeheader()
                for record in self._cache.values():
                    writer.writerow(record)
            logging.info(f"Saved {len(self._cache)} records to CSV file")
        except Exception as e:
            logging.error(f"Error saving to CSV file: {e}")

    async def update_metadata(self, symbol: str, refinitiv_data: Optional[Dict[str, str]] = None, ib_data: Optional[Dict[str, str]] = None):
        async with self._cache_lock:
            now = datetime.utcnow().isoformat()
            record = self._cache.get(symbol, {field: '' for field in FIELDNAMES})
            record['symbol'] = symbol

            if not record.get('created_time'):
                record['created_time'] = now

            if refinitiv_data:
                record['refinitiv_title'] = refinitiv_data.get('title', '')
                record['refinitiv_ric'] = refinitiv_data.get('ric', '')

            if ib_data:
                record['ib_conid'] = str(ib_data.get('conId', ''))
                record['ib_primary_exchange'] = ib_data.get('primaryExchange', '')
                record['ib_currency'] = ib_data.get('currency', '')
                record['ib_long_name'] = ib_data.get('description', '')
                record['ib_exchange'] = ib_data.get('exchange', '')
                record['ib_price_magnifier'] = ib_data.get('multiplier', '')
                record['ib_under_sec_type'] = ib_data.get('secType', '')
            record['update_time'] = now
            self._cache[symbol] = record
            self._save_to_csv()
            logging.info(f"Updated metadata for symbol: {symbol}")

    async def get_metadata(self, symbol: str) -> Optional[Dict[str, str]]:
        async with self._cache_lock:
            return self._cache.get(symbol)

    async def get_all_metadata(self) -> List[Dict[str, str]]:
        async with self._cache_lock:
            return list(self._cache.values())
