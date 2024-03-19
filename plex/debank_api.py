import json
import os.path
import typing
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Tuple

import aiohttp
import asyncio

import pandas as pd
import yaml
from pandas import DataFrame

from utils.db import CsvDB


class DebankAPI:
    endpoints = ["all_complex_protocol_list", "all_token_list", "all_nft_list"]
    api_url = "https://pro-openapi.debank.com/v1"
    def __init__(self, db: CsvDB):
        with open(os.path.join(os.sep, os.getcwd(), 'config', 'params.yaml'), "r") as ymlfile:
            self.config = yaml.safe_load(ymlfile)

        self.db = db

    async def fetch_position_snapshot(self, address: str, debank_key: str, write_to_json=True) -> dict:
        '''
        Fetches the position snapshot for a given address from the Debank API
        Stores the result in a json file if write_to_json is True
        Parses the result into a pandas DataFrame and returns it
        '''

        async def call_position_endpoint(endpoint: str) -> typing.Any:
            async with session.get(url=f'{self.api_url}/{endpoint}',
                                   headers={
                                       "accept": "application/json",
                                       "AccessKey": debank_key,
                                   },
                                   params={"id": address}) as response:
                return await response.json()

        now_time = datetime.now(tz=timezone.utc).timestamp()
        async with aiohttp.ClientSession() as session:
            json_results = await asyncio.gather(*[call_position_endpoint(f'user/{endpoint}')
                                       for endpoint in self.endpoints])

        dict_result = {'timestamp': now_time} | dict(zip(self.endpoints, json_results))
        if write_to_json:
            self.db.insert_snapshot(dict_result, address)

        return dict_result

    def query_explain_data(self, address: str, start_date: datetime, end_date: datetime = datetime.now()) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        start_snapshot, end_snapshot, transactions = self.db.query_explain_data(address, start_date, end_date)
        return self.parse_snapshot(start_snapshot), self.parse_snapshot(end_snapshot), pd.DataFrame(transactions)

    def parse_snapshot(self, dict_result: dict) -> pd.DataFrame:
        if not dict_result:
            return pd.DataFrame()
        timestamp = int(dict_result.pop('timestamp'))
        res_list = sum(
            (
                getattr(self, f'parse_{endpoint}')(res)
                for endpoint, res in dict_result.items()
            ),
            [],
        )
        df_result = pd.DataFrame(res_list)
        df_result['timestamp'] = timestamp
        df_result = df_result[~df_result['protocol'].isin(self.config['plex']['redundant_protocols'])]
        return df_result

    # TODO:
    # async def fetch_transactions(self, address: str, start_time: int) -> list:
    #     cur_time = start_time
    #     data = []
    #     async with aiohttp.ClientSession() as session:
    #         while True:
    #             async with session.get(f'{self.api_url}/user/all_history_list', headers=self.headers,
    #                                    params={"id": address, "start_time": cur_time, "page_count": 20}) as response:
    #                 try:
    #                     temp = await response.json()
    #                     data += temp
    #                     cur_time = temp['time_at'] + 1
    #                 except Exception as e:
    #                     print(f'Error: {e}')
    #                     break
    #     return data

    @staticmethod
    def parse_all_complex_protocol_list(snapshot: list) -> list:
        result = []
        for protocol in snapshot:
            for portfolio_item in protocol['portfolio_item_list']:
                for bucket_type, positions in portfolio_item['detail'].items():
                    if isinstance(positions, list):
                        result.extend(
                            {
                                'chain': protocol['chain'],
                                'protocol': protocol['name'],
                                # 'description': portfolio_item['detail']['description'],
                                'hold_mode': portfolio_item['name'],
                                'type': bucket_type,
                                'asset': position['symbol'],
                                'amount': (-1 if 'borrow' in bucket_type else 1)
                                * position['amount'],
                                'price': position['price'],
                                'value': (-1 if 'borrow' in bucket_type else 1)
                                * position['amount']
                                * position['price'],
                            }
                            for position in positions
                        )
        return result

    @staticmethod
    def parse_all_token_list(snapshot: list) -> list:
        return [
            {
                'chain': position['chain'],
                'protocol': 'wallet',
                # 'description': portfolio_item['detail']['description'],
                'hold_mode': 'cash',
                'type': 'cash',
                'asset': position['symbol'],
                'amount': position['amount'],
                'price': position['price'],
                'value': position['amount'] * position['price'],
            }
            for position in snapshot
            if position['is_verified'] and (position['price'] > 0)
        ]

    @staticmethod
    def parse_all_nft_list(snapshot: list) -> list:
        return [
            {
                'chain': position['chain'],
                'protocol': position['name'],
                #'description': portfolio_item['detail']['description'],
                'hold_mode': 'cash',
                'type': 'nft',
                'asset': position['name'],
                'amount': position['amount'],
                'price': position['usd_price'] if 'usd_price' in position else 0.0,
                'value': position['amount'] * position['usd_price'],
            }
            for position in snapshot
            if ('usd_price' in position) and (position['usd_price'] > 0)
        ]

    @staticmethod
    def parse_all_history_list(snapshot: dict) -> list:
        result = []
        for tx in snapshot['history_list']:
            if not tx['is_scam']:
                def append_leg(leg, side):
                    return {'timestamp': 0,
                            'chain': tx['chain'],
                            'protocol': snapshot['project_dict'][tx['project_id']]['name'] if 'project_id' in tx else
                            leg['to_addr' if side == -1 else 'from_addr'],
                            'gas': tx['tx']['usd_gas_fee'] if 'usd_gas_fee' in tx['tx'] else 0.0,
                            'type': tx['cate_id'],
                            'asset': leg['token_id'],
                            'amount': leg['amount'] * side,
                            'price': snapshot['token_dict'][leg['token_id']]['price'],
                            'value': leg['amount'] * snapshot['token_dict'][leg['token_id']]['price'] * side}

                for cur_leg in tx['receives']:
                    result.append(append_leg(cur_leg, 1))
                for cur_leg in tx['sends']:
                    result.append(append_leg(cur_leg, -1))

        return result
    