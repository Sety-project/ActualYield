import copy
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import streamlit as st
import yaml
from pandas import DataFrame


class PnlExplainer:
    def __init__(self):
        self.categories_path = os.path.join(os.getcwd(), 'config', 'categories.yaml')
        with open(self.categories_path, 'r') as f:
            self.categories = yaml.safe_load(f)

    def validate_categories(self, data) -> bool:
        if missing_category := set(data['asset']) - set(self.categories.keys()):
            st.warning(f"Categories need to be updated. Please categorize the following assets: {missing_category}")
            return False
        if missing_underlying := set(self.categories.values()) - set(data['asset']):
            st.warning(f"I need underlying {missing_underlying} to have a position, maybe get some dust? Sorry...")
            return False
        return True

    def explain(self, start_snapshot: pd.DataFrame, end_snapshot: pd.DataFrame) -> DataFrame:
        snapshot_start = start_snapshot.set_index([col for col in start_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        snapshot_end = end_snapshot.set_index([col for col in end_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        data = snapshot_start.join(snapshot_end, how='outer', lsuffix='_start', rsuffix='_end')
        data = data.reset_index()

        if not self.validate_categories(data):
            return pd.DataFrame()

        data['underlying'] = data.apply(lambda x: self.categories[x['asset']], axis=1)
        data['dP'] = data['price_end'] - data['price_start']
        # TODO: messy since we need position on same chain, USD and EUR don't work...need coingecko snap.
        data['dP_underlying'] = data.apply(lambda x: data.loc[data['asset'] == x['underlying'], 'dP'].mean(), axis=1)
        data['dP_basis'] = data['dP'] - data['dP_underlying']
        data['timestamp_start'] = min(data['timestamp_start'])
        data['timestamp_end'] = max(data['timestamp_end'])
        data = data.fillna(0)

        # now we add one row per pnl bucket, this ensures nice subtotal rendering
        delta_pnl = copy.deepcopy(data)
        delta_pnl['pnl_bucket'] = 'delta'
        delta_pnl['pnl'] = data['dP_underlying'] * data['amount_start']

        basis_pnl = copy.deepcopy(data)
        basis_pnl['pnl_bucket'] = 'basis'
        basis_pnl['pnl'] = data['dP_basis'] * data['amount_start']

        amt_chng_pnl = copy.deepcopy(data)
        amt_chng_pnl['pnl_bucket'] = 'amt_chng'
        amt_chng_pnl['pnl'] = (data['amount_end'] - data['amount_start']) * data['price_end']

        assert (data['value_end'] - data['value_start'] - delta_pnl['pnl'] - basis_pnl['pnl'] - amt_chng_pnl['pnl']).apply(abs).max() < 1, \
            "something doesn't add up..."

        result = pd.concat([delta_pnl, basis_pnl, amt_chng_pnl], axis=0, ignore_index=True)
        result['timestamp_end'] = result['timestamp_end'].apply(lambda x: datetime.fromtimestamp(x, tz=timezone.utc))
        result['timestamp_start'] = result['timestamp_start'].apply(
            lambda x: datetime.fromtimestamp(x, tz=timezone.utc))

        return result

    def format_transactions(self, start_snapshot_timestamp: int, end_snapshot_timestamp: int, transactions: pd.DataFrame) -> pd.DataFrame:
        tx_pnl = transactions.groupby(by=['chain', 'protocol', 'type', 'asset']).sum()[['pnl', 'gas']].reset_index()
        tx_pnl['pnl_bucket'] = 'tx_pnl'
        tx_pnl['timestamp_start'] = start_snapshot_timestamp
        tx_pnl['timestamp_end'] = end_snapshot_timestamp
        tx_pnl['hold_mode'] = tx_pnl['type']
        tx_pnl['underlying'] = tx_pnl['asset']

        return tx_pnl
