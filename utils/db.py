import asyncio
import json
import logging
import os
import sys
import typing
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ClientError
import pandas as pd
import sqlite3

from pandas import DataFrame


TableType = typing.NewType('TableType', typing.Literal["snapshots", "transactions"])

class RawDataDB(ABC):
    '''
    Abstract class for RawDataDB, where we put raw data in cold storage.
    '''
    @staticmethod
    def build_RawDataDB(config: dict, secrets: dict):
        return getattr(sys.modules[__name__], config['type'])(config, secrets)

    @abstractmethod
    def query_table(self, address: str, timestamp: int, table_name: TableType) -> dict:
        raise NotImplementedError

    @abstractmethod
    def insert_table(self, dict_result: dict, address: str, table_name: TableType) -> None:
        raise NotImplementedError

    @abstractmethod
    def all_timestamps(self, address: str, table_name: TableType) -> list[int]:
        raise NotImplementedError


class LocalJsonRawDataDB(RawDataDB):

    def __init__(self, config: dict):
        self.data_dir = config['data_dir']

    def query_table(self, address: str, timestamp: int, table_name: TableType) -> dict:
        with open(os.path.join(self.data_dir, f'{table_name}_{address}_{timestamp}.json'), 'r') as f:
            return json.load(f)

    def insert_table(self, dict_result: dict, address: str, table_name: TableType) -> None:
        if 'start_timestamp' in dict_result and 'end_timestamp' in dict_result:
            with open(os.path.join(os.sep, self.data_dir, f"{table_name}_{address}_{dict_result['start_timestamp']}_{dict_result['end_timestamp']}.json"), 'w') as f:
                json.dump(dict_result, f)
        else:
            with open(os.path.join(os.sep, self.data_dir, f"{table_name}_{address}_{int(dict_result['timestamp'])}.json"), 'w') as f:
                json.dump(dict_result, f)

    def all_timestamps(self, address: str, table_name: TableType) -> list[int]:
        return [int(file.split('_')[2].split('.')[0]) for file in os.listdir(self.data_dir)
                if file.startswith(table_name) and file.endswith('.json') and address in file]


class S3JsonRawDataDB(RawDataDB):
    def __init__(self, config: dict, secrets: dict):
        self.bucket_name = config['bucket_name']
        self.data_dir = config['data_dir']
        self.connection = boto3.client('s3',
                                                         aws_access_key_id=secrets['AWS_ACCESS_KEY_ID'],
                                                         aws_secret_access_key=secrets['AWS_SECRET_ACCESS_KEY'])

    def query_table(self, address: str, timestamp: int, table_name: TableType) -> dict:
        key = os.path.join(self.data_dir, f'{table_name}_{address}_{timestamp}.json')
        response = self.connection.get_object(Bucket=self.bucket_name, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))

    def insert_table(self, dict_result: dict, address: str, table_name: TableType) -> None:
        if 'start_timestamp' in dict_result and 'end_timestamp' in dict_result:
            key = os.path.join(self.data_dir, f"{table_name}_{address}_{dict_result['start_timestamp']}_{dict_result['end_timestamp']}.json")
        else:
            key = os.path.join(self.data_dir, f"{table_name}_{address}_{int(dict_result['timestamp'])}.json")
        json_data = json.dumps(dict_result)
        self.connection.put_object(Bucket=self.bucket_name, Key=key, Body=json_data)

    def all_timestamps(self, address: str, table_name: TableType) -> list[int]:
        '''in fact returns filenames'''
        response = self.connection.list_objects_v2(Bucket=self.bucket_name, Prefix=self.data_dir)
        return [obj['Key'] for obj in response.get('Contents', [])
                 if table_name in obj['Key'] and obj['Key'].endswith('.json') and address in obj['Key']]


class SQLiteDB:
    def __init__(self, config: dict, secrets: dict):
        if 'bucket_name' in config and 'remote_file' in config:
            # if bucket_name is in config, we are using s3 and download the file to ~
            self.data_location = {'bucket_name': config['bucket_name'],
                                  'remote_file': config['remote_file'],
                                  'local_file': os.path.join(os.sep, os.getcwd(), 'plex.db')}
            self.secrets = secrets

            s3 = boto3.client('s3',
                              aws_access_key_id=secrets['AWS_ACCESS_KEY_ID'],
                              aws_secret_access_key=secrets['AWS_SECRET_ACCESS_KEY'])
            # check if the file exists in s3
            try:
                s3.download_file(self.data_location['bucket_name'],
                                 self.data_location['remote_file'],
                                 self.data_location['local_file'])
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logging.warning(f'Creating new {self.data_location["local_file"]}')
                    # create a new file, or overwrite existing one
                    with open(self.data_location['local_file'], 'w') as f:
                        f.write('')
                else:
                    raise e
            local_file = self.data_location['local_file']
        elif 'data_dir' in config:
            # if not, we are using local and the file is already in the data_dir
            data_dir = os.path.join(os.sep, Path.home(), config['data_dir'])
            if not os.path.isdir(data_dir):
                os.mkdir(data_dir)
                os.chmod(data_dir, 0o777)
            local_file = os.path.join(data_dir, 'plex.db')
        else:
            raise ValueError('config must contain either bucket_name and filename, or data_dir')
        # self.engine = st.experimental_connection(config['data_dir'], type=config['type'], autocommit=True)
        self.conn = sqlite3.connect(local_file, check_same_thread=False)
        os.chmod(local_file, 0o777)
        self.cursor = self.conn.cursor()

    def last_updated(self, address: str, table_name: TableType) -> tuple[datetime, pd.DataFrame]:
        if all_timestamps := self.all_timestamps(address, table_name):
            timestamp = max(all_timestamps)
            latest_table = self.query_table_at([address], timestamp, table_name)
            return datetime.fromtimestamp(timestamp, tz=timezone.utc), latest_table
        else:
            return datetime(1970, 1, 1, tzinfo=timezone.utc), pd.DataFrame()

    def upload_to_s3(self):
        s3 = boto3.client('s3',
                          aws_access_key_id=self.secrets['AWS_ACCESS_KEY_ID'],
                          aws_secret_access_key=self.secrets['AWS_SECRET_ACCESS_KEY'])
        s3.upload_file(self.data_location['local_file'], self.data_location['bucket_name'],
                       self.data_location['remote_file'])

    def insert_table(self, df: pd.DataFrame, table_name: TableType) -> None:
        for address, data in df.groupby('address'):
            table = f"{table_name}_{address}"
            data.drop(columns='address').to_sql(table, self.conn, if_exists='append', index=False)

    def query_table_at(self, addresses: list[str], timestamp: int, table_name: TableType) -> pd.DataFrame:
        return pd.concat([pd.read_sql_query(f'SELECT * FROM {table_name}_{address} WHERE timestamp = {timestamp}', self.conn)
                            for address in addresses], ignore_index=True, axis=0)

    def query_table_between(self, addresses: list[str], start_timestamp: int, end_timestamp: int, table_name: TableType) -> pd.DataFrame:
        return pd.concat([pd.read_sql_query(f'SELECT * FROM {table_name}_{address} WHERE timestamp BETWEEN {start_timestamp} AND {end_timestamp}',self.conn)
                          for address in addresses], ignore_index=True, axis=0)
    
    def all_timestamps(self, address: str, table_name: TableType) -> list[int]:
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}_{address}';")
        if not self.cursor.fetchall():
            return []
        self.cursor.execute(f'SELECT DISTINCT timestamp FROM {table_name}_{address}')
        rows = self.cursor.fetchall()
        return [row[0] for row in rows]
    
    def query_categories(self) -> dict:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", self.conn)
        if 'categories' not in tables.values:
            pd.DataFrame(columns=['asset', 'underlying']).to_sql('categories', self.conn, index=False)
            return {}
        return pd.read_sql_query('SELECT * FROM categories', self.conn).set_index('asset')['underlying'].to_dict()

    def overwrite_categories(self, categories: dict) -> None:
        # if True:
        #     with open(os.path.join(os.getcwd(), 'config', 'categories_SAVED.yaml'), 'r') as file:
        #         categories = yaml.safe_load(file)
        pd.DataFrame({'asset':categories.keys(), 'underlying': categories.values()}).to_sql('categories', self.conn, index=False, if_exists='replace')
        self.conn.commit()