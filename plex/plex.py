import copy
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st
import yaml
from pandas import DataFrame
from utils.coingecko import ScannerAPI


class PnlExplainer:
    def __init__(self, categories: dict[str, str], alchemy_key: str = None):
        self.categories = categories
        self.etherscan_api = ScannerAPI(alchemy_key)

    def underlying(self, asset: str) -> str:
        categories = {key.lower(): value for key, value in self.categories.items()}
        return categories.get(asset.lower(), asset)

    def validate_categories(self, data) -> None:
        if missing_category := set(data['asset']) - set(self.categories.keys()):
            st.error(f"Categories need to be updated. Please categorize the following assets: {missing_category}")
            st.stop()

    def explain(self, start_snapshot: pd.DataFrame, end_snapshot: pd.DataFrame) -> DataFrame:
        snapshot_start = start_snapshot.set_index([col for col in start_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        snapshot_end = end_snapshot.set_index([col for col in end_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        data = snapshot_start.join(snapshot_end, how='outer', lsuffix='_start', rsuffix='_end')
        common_pos = data[data.index.isin(set(snapshot_start.index) & set(snapshot_end.index))].reset_index()
        before_pos = data[data.index.isin(set(snapshot_start.index) - set(snapshot_end.index))].reset_index()
        after_pos = data[data.index.isin(set(snapshot_end.index) - set(snapshot_start.index))].reset_index()

        self.validate_categories(common_pos)

        common_pos['underlying'] = common_pos['asset'].apply(self.underlying)
        before_pos['underlying'] = before_pos['asset'].apply(self.underlying)
        after_pos['underlying'] = after_pos['asset'].apply(self.underlying)
        # TODO: messy since we need position on same chain, USD and EUR don't work...need coingecko snap.
        common_pos[['P_underlying_start', 'P_underlying_end']] = common_pos.apply(lambda x: common_pos.loc[common_pos['asset'] == x['underlying'], ['price_start', 'price_end']].mean(), axis=1)
        # data['dP_basis'] = data['dP'] / data['dP_underlying']
        common_pos = common_pos.fillna(0)

        # delta is underlying-equivalent amount * dP
        delta_pnl = copy.deepcopy(common_pos)
        delta_pnl['pnl_bucket'] = 'delta'
        delta_pnl['pnl'] = (common_pos['P_underlying_end'] - common_pos['P_underlying_start']) * common_pos['amount_start'] * common_pos['price_start'] / common_pos['P_underlying_start']

        # basis is the rest
        basis_pnl = copy.deepcopy(common_pos)
        basis_pnl['pnl_bucket'] = 'basis'
        basis_pnl['pnl'] = common_pos['amount_start']*(common_pos['price_end'] - common_pos['price_start']) - delta_pnl['pnl']

        amt_chng_pnl = copy.deepcopy(common_pos)
        amt_chng_pnl['pnl_bucket'] = 'amt_chng'
        amt_chng_pnl['pnl'] = (common_pos['amount_end'] - common_pos['amount_start']) * common_pos['price_end']

        before_pos['pnl_bucket'] = 'amt_chng'
        before_pos['pnl'] = - before_pos['amount_start'] * before_pos['price_start']

        after_pos['pnl_bucket'] = 'amt_chng'
        after_pos['pnl'] = after_pos['amount_end'] * after_pos['price_end']

        assert (common_pos['value_end'] - common_pos['value_start'] - delta_pnl['pnl'] - basis_pnl['pnl'] - amt_chng_pnl['pnl']).apply(abs).max() < 1, \
            "something doesn't add up..."

        result = pd.concat([delta_pnl, basis_pnl, amt_chng_pnl, before_pos, after_pos], axis=0, ignore_index=True)
        result['timestamp_end'] = datetime.fromtimestamp(max(common_pos['timestamp_end']), tz=timezone.utc)
        result['timestamp_start'] = datetime.fromtimestamp(min(common_pos['timestamp_start']), tz=timezone.utc)

        return result

    def format_transactions(self, start_snapshot_timestamp: int, end_snapshot_timestamp: int, transactions: pd.DataFrame) -> pd.DataFrame:
        tx_pnl = transactions[~transactions['id'].duplicated()]
        tx_pnl['pnl_bucket'] = 'tx_pnl'
        tx_pnl['timestamp_start'] = start_snapshot_timestamp
        tx_pnl['timestamp_end'] = end_snapshot_timestamp
        tx_pnl['hold_mode'] = tx_pnl['type']
        tx_pnl['asset'] = tx_pnl.apply(
                lambda x: self.etherscan_api.get_token_symbol(x['asset'], x['chain']), axis=1)
        tx_pnl['underlying'] = tx_pnl['asset'].apply(self.underlying)

        return tx_pnl
