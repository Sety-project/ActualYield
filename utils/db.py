import asyncio
import json
import os
import typing
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import pandas as pd
import sqlite3


class RawDataDB:
    def __init__(self):
        self.data_dir = os.path.join(os.sep, os.getcwd(), 'data')
        if not os.path.isdir(self.data_dir):
            os.mkdir(self.data_dir)
            os.chmod(self.data_dir, 0o777)

    def query_snapshot(self, address: str, timestamp: int) -> dict:
        with open(os.path.join(self.data_dir, f'snapshot_{address}_{timestamp}.json'), 'r') as f:
            return json.load(f)

    def insert_snapshot(self, dict_result: dict, address: str) -> None:
        with open(os.path.join(os.sep, self.data_dir, f"snapshot_{address}_{int(dict_result['timestamp'])}.json"), 'w') as f:
            json.dump(dict_result, f)

    def all_timestamps(self, address: str) -> list[int]:
        return [int(file.split('_')[2].split('.')[0]) for file in os.listdir(self.data_dir)
                if file.startswith('snapshot') and file.endswith('.json') and address in file]


class PlexDB(ABC):
    @abstractmethod
    def query_snapshot(self, address: str, timestamp: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def insert_snapshot(self, df: pd.DataFrame, address: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def all_timestamps(self, address: str) -> list[int]:
        raise NotImplementedError

    def query_explain_data(self, address: str, start_date: datetime, end_date: datetime = datetime.now()) -> dict[str, pd.DataFrame]:
        timestamps = self.all_timestamps(address)
        start_timestamp = next((ts for ts in sorted(timestamps, reverse=True)
                                if ts <= start_date.timestamp()), min(timestamps))
        end_timestamp = next((ts for ts in sorted(timestamps, reverse=False)
                              if ts >= end_date.timestamp()), max(timestamps))

        start_snapshot = self.query_snapshot(address, start_timestamp)
        end_snapshot = self.query_snapshot(address, end_timestamp)
        transactions = []

        return {'start_snapshot': start_snapshot,
                'end_snapshot': end_snapshot}

    def last_updated(self, address: str) -> tuple[datetime, pd.DataFrame]:
        if all_timestamps := self.all_timestamps(address):
            timestamp = max(all_timestamps)
            latest_snapshot = self.query_snapshot(address, timestamp)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc), latest_snapshot
        else:
            return datetime(1970, 1, 1, tzinfo=timezone.utc), {}


class SQLiteDB(PlexDB):
    plex_schema = {'chain': 'TEXT',
                   'protocol': 'TEXT',
                   'hold_mode': 'TEXT',
                   'type': 'TEXT',
                   'asset': 'TEXT',
                   'amount': 'REAL',
                   'price': 'REAL',
                   'value': 'REAL',
                   'timestamp': 'INTEGER'}
    def __init__(self):
        data_dir = os.path.join(os.sep, os.getcwd(), 'data')
        if not os.path.isdir(data_dir):
            os.mkdir(data_dir)
            os.chmod(data_dir, 0o777)
        self.conn = sqlite3.connect(os.path.join(data_dir, 'plex.db'))
        self.cursor = self.conn.cursor()

    def insert_snapshot(self, df: pd.DataFrame) -> None:
        for address, data in df.groupby('address'):
            table_name = f"plex_data_{address}"
            data.drop(columns='address').to_sql(table_name, self.conn, if_exists='append', index=False)
            self.conn.commit()

    def query_snapshot(self, address: str, timestamp: int) -> pd.DataFrame:
        table_name = f"plex_data_{address}"
        return pd.read_sql_query(f'SELECT * FROM {table_name} WHERE timestamp = {timestamp}',
                          self.conn)

    def all_timestamps(self, address: str) -> list[int]:
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='plex_data_{address}';")
        if not self.cursor.fetchall():
            return []
        self.cursor.execute(f'SELECT DISTINCT timestamp FROM plex_data_{address}')
        rows = self.cursor.fetchall()
        return [row[0] for row in rows]

