import copy
import typing
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import pandas as pd
import requests
import aiohttp
import streamlit as st

from utils.async_utils import safe_gather
from utils.db import RawDataDB, SQLiteDB


class DebankAPI:
    endpoints = ["all_complex_protocol_list", "all_token_list", "all_nft_list"]
    api_url = "https://pro-openapi.debank.com/v1"
    def __init__(self, json_db: RawDataDB, plex_db: SQLiteDB, parameters: Dict[str, Any]):
        self.parameters = parameters
        self.json_db: RawDataDB = json_db
        self.plex_db: SQLiteDB = plex_db

    def get_credits(self) -> float:
        response = requests.get(f'{self.api_url}/account/units',
                                headers={
                                    "accept": "application/json",
                                    "AccessKey": self.parameters['profile']['debank_key'],
                                })
        return response.json()['balance']
    async def _fetch_snapshot(self, address: str, write_to_json=True) -> dict:
        '''
        Fetches the position snapshot for a given address from the Debank API
        Stores the result in a json file if write_to_json is True
        Parses the result into a pandas DataFrame and returns it
        '''

        async def call_position_endpoint(endpoint: str) -> typing.Any:
            async with session.get(url=f'{self.api_url}/{endpoint}',
                                   headers={
                                       "accept": "application/json",
                                       "AccessKey": self.parameters['profile']['debank_key'],
                                   },
                                   params={"id": address}) as response:
                return await response.json()

        now_time = datetime.now(tz=timezone.utc).timestamp()
        async with aiohttp.ClientSession() as session:
            json_results = await safe_gather([call_position_endpoint(f'user/{endpoint}')
                                       for endpoint in self.endpoints],
                                             n=self.parameters['run_parameters']['async']['gather_limit'])

        dict_result = {'timestamp': now_time, 'address': address} | dict(zip(self.endpoints, json_results))
        if write_to_json:
            self.json_db.insert_table(dict_result, address, "snapshots")

        return dict_result

    async def fetch_snapshot(self, address: str, refresh: bool) -> pd.DataFrame:
        '''
        fetch from debank if not recently updated, or db if recently updated or refresh=False
        then write to json to disk
        returns parsed latest snapshot summed across all addresses
        only update once every 'update_frequency' minutes
        '''
        updated_at, snapshot = self.plex_db.last_updated(address, "snapshots")

        # retrieve cache for addresses that have been updated recently, and always if refresh=False
        max_updated = datetime.now(tz=timezone.utc) - timedelta(
            minutes=self.parameters['plex']['update_frequency'])
        if refresh:
            if updated_at < max_updated:
                snapshot_dict = await self._fetch_snapshot(address, write_to_json=True)
                snapshot = self.parse_snapshot(snapshot_dict)
                self.plex_db.insert_table(snapshot, "snapshots")
            else:
                st.warning(
                    f"We only update once every {self.parameters['plex']['update_frequency']} minutes. {address} not refreshed")
        return snapshot

    def parse_snapshot(self, dict_input: dict) -> pd.DataFrame:
        if not dict_input:
            return pd.DataFrame()
        dict_result = copy.deepcopy(dict_input)
        timestamp = int(dict_result.pop('timestamp'))
        address = dict_result.pop('address')
        res_list = sum(
            (
                getattr(self, f'parse_{endpoint}')(res)
                for endpoint, res in dict_result.items()
            ),
            [],
        )
        df_result = pd.DataFrame(res_list)
        df_result['timestamp'] = timestamp
        df_result['address'] = address
        df_result = df_result[~df_result['protocol'].isin(self.parameters['plex']['redundant_protocols'])]
        return df_result

    async def _fetch_transactions(self, address: str, start_timestamp: int, end_timestamp: int, write_to_json=False) -> list:
        '''
        returns {tx_hash: {timestamp instead of taime_atm, the rest}}
        '''
        cur_timestamp = end_timestamp
        data = {'cate_dict': {}, 'cex_dict': {}, 'history_list': [], 'project_dict': {}, 'token_dict': {}}
        while cur_timestamp >= start_timestamp:
            try:
                response = requests.get(f'{self.api_url}/user/all_history_list',
                                   headers={
                                       "accept": "application/json",
                                       "AccessKey": self.parameters['profile']['debank_key'],
                                   },
                                   params={"id": address, "start_time": int(cur_timestamp), "page_count": 20})
                temp = response.json()
                cur_timestamp = min(cur_timestamp, min(x['time_at'] for x in temp['history_list']) -1)
                for key, value in temp.items():
                    if isinstance(value, dict):
                        data[key] |= value
                    elif isinstance(value, list):
                        data[key] += value
                    else:
                        raise ValueError(f'Unexpected type {type(value)}')
            except Exception as e:
                print(f'Error: {e}')
                break
        data = {'start_timestamp': end_timestamp, 'end_timestamp': end_timestamp, 'tx_list': data}
        if write_to_json:
            self.json_db.insert_table(data, address, "transactions")

        return data['tx_list']

    async def fetch_transactions(self, address: str) -> pd.DataFrame:
        '''
        fetch from debank if not recently updated, or db if recently updated or refresh=False
        then write to json to disk
        returns parsed latest snapshot summed across all addresses
        only update once every 'update_frequency' minutes
        '''
        updated_at, _ = self.plex_db.last_updated(address, "transactions")

        transactions_list = await self._fetch_transactions(address,
                                                           start_timestamp=int(updated_at.timestamp()),
                                                           end_timestamp=int(datetime.now().timestamp()),
                                                           write_to_json=True)
        transactions = self.parse_all_history_list(transactions_list)
        if not transactions.empty:
            transactions['address'] = address
            transactions = transactions[~transactions['id'].duplicated()]
            self.plex_db.insert_table(transactions, "transactions")
        return transactions

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
    def parse_all_history_list(transactions: list) -> pd.DataFrame:
        result = []
        for tx in transactions['history_list']:
            if not tx['is_scam']:
                def append_leg(leg, side):
                    result = {'id': tx['id'],
                              'timestamp': tx['time_at'],
                              'chain': tx['chain'],
                              'protocol': transactions['project_dict'][tx['project_id']]['name'] if tx[
                                  'project_id'] else
                              leg['to_addr' if side == -1 else 'from_addr'],
                              'gas': tx['tx']['usd_gas_fee'] if 'usd_gas_fee' in tx['tx'] else 0.0,
                              'type': tx['tx']['name'],
                              'asset': leg['token_id'],
                              'amount': leg['amount'] * side}
                    if leg['token_id'] in transactions['token_dict']:
                        if ('price' in transactions['token_dict'][leg['token_id']]) and transactions['token_dict'][leg['token_id']]['price']:
                            result['price'] = transactions['token_dict'][leg['token_id']]['price']
                            result['pnl'] = leg['amount'] * result['price'] * side
                    return result


                if 'receives' in tx:
                    for cur_leg in tx['receives']:
                        result.append(append_leg(cur_leg, 1))
                if 'sends' in tx:
                    for cur_leg in tx['sends']:
                        result.append(append_leg(cur_leg, -1))

        df = pd.DataFrame(result)
        if not df.empty:
            df['pnl'] = df['pnl'] - df['gas']
        return df
    