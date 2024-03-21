import copy
import os
import pandas as pd
import streamlit as st
import yaml


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

    def explain(self, start_snapshot: pd.DataFrame, end_snapshot: pd.DataFrame, transactions: pd.DataFrame = pd.DataFrame()) -> pd.DataFrame:
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

        return pd.concat([delta_pnl, basis_pnl, amt_chng_pnl], axis=0, ignore_index=True)
