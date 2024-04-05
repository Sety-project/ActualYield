import asyncio
import copy
import sys
from datetime import timedelta, date, datetime, timezone, time
from hashlib import sha256

import pandas as pd
import streamlit as st
import yaml
import plotly.express as px

from plex.plex import PnlExplainer
from utils.async_utils import safe_gather
from utils.db import SQLiteDB, RawDataDB, PlexDB
from plex.debank_api import DebankAPI

assert (sys.version_info >= (3, 10)), "Please use Python 3.10 or higher"

if 'set_config' not in st.session_state:  # hack to have it run only once, and before any st is called (crash otherwise..)
    st.set_page_config(layout="wide")
    st.session_state.set_config =True

from utils.streamlit_utils import load_parameters, prompt_plex_interval, display_pivot, download_button, \
    download_db_button

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()

# TODO: no pb reloading each time ? bc of sql
if 'plex_db' not in st.session_state:
    # tamper with the db file name to add debank key
    plex_db_params = copy.deepcopy(st.session_state.parameters['input_data']['plex_db'])
    plex_db_params['remote_file'] = plex_db_params['remote_file'].replace('.db',
                                                                          f"_{st.session_state.parameters['profile']['debank_key']}.db")
    st.session_state.plex_db: PlexDB = SQLiteDB(plex_db_params, st.secrets)
    raw_data_db: RawDataDB = RawDataDB.build_RawDataDB(st.session_state.parameters['input_data']['raw_data_db'], st.secrets)
    st.session_state.api = DebankAPI(json_db=raw_data_db,
                                     plex_db=st.session_state.plex_db,
                                     parameters=st.session_state.parameters)
    st.session_state.pnl_explainer = PnlExplainer()

addresses = st.session_state.parameters['profile']['addresses']
risk_tab, pnl_tab = st.tabs(
    ["risk", "pnl"])

with st.sidebar.form("snapshot_form"):
    refresh = st.form_submit_button("fetch from debank", help="fetch from debank costs credits !")
    if refresh:
        debank_credits = st.session_state.api.get_credits()
    all_fetch = asyncio.run(safe_gather([st.session_state.api.fetch_snapshot(address, refresh=refresh)
                                         for address in addresses] +
                                        [st.session_state.api.fetch_transactions(address)
                                         for address in addresses if refresh],
                                        n=st.session_state.parameters['run_parameters']['async']['gather_limit']))
    if refresh:
        st.write(f"Debank credits used: {(debank_credits-st.session_state.api.get_credits())*200/1e6} $")
        st.session_state.plex_db.upload_to_s3()

    snapshots = all_fetch[:len(addresses)]
    st.session_state.snapshot = pd.concat(snapshots, axis=0, ignore_index=True)

download_db_button(st.session_state.plex_db, file_name='snapshot.db', label='Download database')

with risk_tab:
    # dynamic categorization
    if 'snapshot' in st.session_state:
        if missing_category := set(st.session_state.snapshot['asset']) - set(st.session_state.pnl_explainer.categories.keys()):
            st.warning(f"New underlyings {missing_category} -> Edit 'underlying' below to group exposures by underlying")

        # display risk
        st.write("Risk pivot table: group exposures by underlying")
        st.session_state.snapshot['underlying'] = st.session_state.snapshot['asset'].map(
            st.session_state.pnl_explainer.categories)
        # risk = copy.deepcopy(st.session_state.snapshot)
        # risk['value'] = risk['value'] / 1000
        display_pivot(st.session_state.snapshot,
                      rows=['underlying', 'asset', 'chain', 'protocol'],
                      columns=['address'],
                      values=['value'],
                      hidden=['hold_mode', 'type', 'price', 'amount'])

        download_button(st.session_state.snapshot, file_name='snapshot.csv', label='Download snapshot')

        with st.form("categorization_form"):
            # categorization
            st.write("Edit 'underlying' below to group exposures by underlying")
            categorization = pd.DataFrame({'underlying': {coin: coin for coin in missing_category}
                                                         | st.session_state.pnl_explainer.categories})
            categorization['exposure'] = st.session_state.snapshot.groupby('asset').sum()['value']
            edited_categorization = st.data_editor(categorization, use_container_width=True)['underlying'].to_dict()
            if st.form_submit_button("Override categorization"):
                st.session_state.pnl_explainer.categories = edited_categorization
                with open(st.session_state.pnl_explainer.categories_path, 'w') as f:
                    yaml.dump(edited_categorization, f)
                st.success("Categories updated (not exposure!)")

with pnl_tab:
    start_timestamp, end_timestamp = prompt_plex_interval(st.session_state.plex_db, addresses)

    details_tab, history_tab = st.tabs(["details", "history"])

    with details_tab:
        ## display_pivot plex
        st.subheader("Pnl Explain")

        st.latex(r'PnL_{\text{delta}} = \sum \Delta P_{\text{underlying}} \times N^{\text{start}}')
        st.latex(r'PnL_{\text{basis}} = \sum \Delta (P_{\text{asset}}-P_{\text{underlying}}) \times N^{\text{start}}')
        st.latex(r'PnL_{\text{amt\_chng}} = \sum \Delta N \times P^{\text{end}}')

        start_snapshot = st.session_state.plex_db.query_table_at(addresses, start_timestamp, "snapshots")
        end_snapshot = st.session_state.plex_db.query_table_at(addresses, end_timestamp, "snapshots")
        st.session_state.plex = st.session_state.pnl_explainer.explain(start_snapshot=start_snapshot, end_snapshot=end_snapshot)

        display_pivot(st.session_state.plex,
                      rows=['underlying', 'asset'],
                      columns=['pnl_bucket'],
                      values=['pnl'],
                      hidden=['protocol', 'chain', 'hold_mode', 'type'])

        ## display_pivot transactions
        st.subheader("Transactions")

        transactions = st.session_state.plex_db.query_table_between(addresses, start_timestamp, end_timestamp, "transactions")
        st.session_state.transactions = st.session_state.pnl_explainer.format_transactions(start_timestamp, end_timestamp, transactions)

        display_pivot(st.session_state.transactions,
                      rows=['asset'],
                      columns=['type'],
                      values=['gas', 'pnl'],
                      hidden=['protocol', 'chain'])

        if 'plex' in st.session_state:
            plex_download_col, tx_download_col = st.columns(2)
            with plex_download_col:
                download_button(st.session_state.plex, file_name='plex.csv', label="Download plex data")
            with tx_download_col:
                download_button(st.session_state.transactions, file_name='tx.csv', label="Download tx data")
                st.session_state.transactions.to_csv('tx.csv')

    with history_tab:
        # snapshots
        snapshots_within = st.session_state.plex_db.query_table_between(st.session_state.parameters['profile']['addresses'], start_timestamp, end_timestamp, "snapshots")
        # explains and transactions btw snapshots
        explain_list = []
        transactions_list = []
        for start, end in zip(
                snapshots_within['timestamp'].unique()[:-1],
                snapshots_within['timestamp'].unique()[1:],
        ):
            start_snapshots = snapshots_within[snapshots_within['timestamp'] == start]
            end_snapshots = snapshots_within[snapshots_within['timestamp'] == end]
            explain = st.session_state.pnl_explainer.explain(start_snapshots, end_snapshots)
            explain_list.append(explain)

            # transactions
            transactions = st.session_state.plex_db.query_table_between(addresses, start, end, "transactions")
            transactions = st.session_state.pnl_explainer.format_transactions(start, end, transactions)
            transactions_list.append(transactions)
        explains = pd.concat(explain_list, axis=0, ignore_index=True)
        tx_pnl = pd.concat(transactions_list, axis=0, ignore_index=True)

        '''
        plot timeseries of explain by some staked_columns
        '''
        categoricals = ['underlying', 'asset', 'protocol', 'pnl_bucket', 'chain', 'hold_mode', 'type']
        values = ['pnl']
        rows = ['timestamp_end']
        granularity_field = st.selectbox("granularity field", categoricals, index=0)
        totals = pd.pivot_table(explains, values=values, columns=[granularity_field], index=rows, aggfunc='sum').cumsum()
        totals = totals.stack().reset_index()

        fig = px.bar(totals, x='timestamp_end', y='pnl',
                     color=granularity_field, title='cum_pnl',
                     barmode='stack')
        min_dt = 4*3600 # 4h
        fig.update_traces(width=min_dt*1000)
        st.plotly_chart(fig, use_container_width=True)

        if 'history' in st.session_state:
            download_button(st.session_state.history, file_name='history.csv', label="Download history data")

