import asyncio
import json
import os
import typing
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import Float, DateTime, Connection, Engine, String

from utils.async_utils import async_wrap


class CsvDB:
    '''shameful hack bc we have pb with sqlalchemy'''
    plex_schema = {'chain': String(255),
                   'protocol': String(255),
                   # 'description': portfolio_item['detail']['description'],
                   'hold_mode': String(255),
                   'type': String(255),
                   'asset': String(255),
                   'amount': Float,
                   'price': Float,
                   'value': Float,
                   'updated': DateTime(timezone=True)}

    def __init__(self):
        self.data_dir = os.path.join(os.sep, os.getcwd(), 'data')
        if not os.path.isdir(self.data_dir):
            os.mkdir(self.data_dir)
            os.chmod(self.data_dir, 0o777)
        
    async def insert_snapshot(self, dict_result: dict, address: str) -> None:
        with open(os.path.join(os.sep, self.data_dir, f"snapshot_{address}_{int(dict_result['timestamp'])}.json"), 'w') as f:
            async_wrap(json.dump)(dict_result, f, ensure_ascii=False, indent=4)

    def query_explain_data(self, address: str, start_date: datetime, end_date: datetime = datetime.now()) -> tuple[dict, dict, list]:
        all_timestamps = [int(file) for file in os.listdir(self.data_dir)
            if file.startswith('snapshot') and file.endswith('.json') and address in file]
        start_timestamp = next((ts for ts in sorted(all_timestamps, reverse=True)
                                if ts <= start_date.timestamp()), min(all_timestamps))
        end_timestamp = next((ts for ts in sorted(all_timestamps, reverse=False)
                              if ts >= end_date.timestamp()), max(all_timestamps))

        with open(os.path.join(self.data_dir, f'snapshot_{start_timestamp}.json'), 'r') as f:
            start_snapshot = json.load(f)
        with open(os.path.join(self.data_dir, f'snapshot_{end_timestamp}.json'), 'r') as f:
            end_snapshot = json.load(f)
            
        transactions = []
        
        return start_snapshot, end_snapshot, transactions

    async def last_updated(self, address: str) -> datetime:
        await asyncio.sleep(0)
        if all_timestamps := [
            int(file.split('_')[2].split('.')[0])
            for file in os.listdir(self.data_dir)
            if file.startswith('snapshot')
            and file.endswith('.json')
            and address in file
        ]:
            return datetime.fromtimestamp(max(all_timestamps), tz=timezone.utc)
        else:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)
