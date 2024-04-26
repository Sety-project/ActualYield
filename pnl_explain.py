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
from utils.db import SQLiteDB, RawDataDB
from plex.debank_api import DebankAPI

assert (sys.version_info >= (3, 10)), "Please use Python 3.10 or higher"

if 'set_config' not in st.session_state:  # hack to have it run only once, and before any st is called (crash otherwise..)
    st.set_page_config(layout="wide")
    st.session_state.set_config =True

from utils.streamlit_utils import load_parameters, prompt_plex_interval, display_pivot, download_button, \
    download_db_button, prompt_snapshot_timestamp

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()

# TODO: no pb reloading each time ? bc of sql
if 'plex_db' not in st.session_state:
    # tamper with the db file name to add debank key
    plex_db_params = copy.deepcopy(st.session_state.parameters['input_data']['plex_db'])
    plex_db_params['remote_file'] = plex_db_params['remote_file'].replace('.db',
                                                                          f"_{st.session_state.parameters['profile']['debank_key']}.db")
    st.session_state.plex_db: SQLiteDB = SQLiteDB(plex_db_params, st.secrets)
    raw_data_db: RawDataDB = RawDataDB.build_RawDataDB(st.session_state.parameters['input_data']['raw_data_db'], st.secrets)
    st.session_state.api = DebankAPI(json_db=raw_data_db,
                                     plex_db=st.session_state.plex_db,
                                     parameters=st.session_state.parameters)
    st.session_state.pnl_explainer = PnlExplainer(st.session_state.plex_db.query_categories(),st.secrets['alchemy_key'])

addresses = st.session_state.parameters['profile']['addresses']
risk_tab, risk_history_tab, pnl_tab, pnl_history_tab = st.tabs(
    ["risk", "risk_history", "pnl", "pnl_history"])

with risk_tab:
    with st.form("snapshot_form"):
        historical_tab, refresh_tab = st.columns(2)
        with historical_tab:
            historical = st.form_submit_button("fetch historical date", help="fetch from db")
            timestamp = prompt_snapshot_timestamp(st.session_state.plex_db, addresses)
        with refresh_tab:
            if refresh := st.form_submit_button("fetch live from debank", help="fetch from debank costs credits !"):
                timestamp = int(datetime.now().timestamp())
                debank_credits = st.session_state.api.get_credits()

    if refresh or historical:
        all_fetch = asyncio.run(
            safe_gather([st.session_state.api.fetch_snapshot(address, refresh=refresh, timestamp=timestamp)
                         for address in addresses] +
                        [st.session_state.api.fetch_transactions(address)
                         for address in addresses if refresh],
                        n=st.session_state.parameters['run_parameters']['async']['gather_limit']))
        if refresh:
            st.write(f"Debank credits used: {(debank_credits - st.session_state.api.get_credits()) * 200 / 1e6} $")
            st.session_state.plex_db.upload_to_s3()

        snapshots = all_fetch[:len(addresses)]
        st.session_state.snapshot = pd.concat(snapshots, axis=0, ignore_index=True)

        download_db_button(st.session_state.plex_db, file_name='snapshot.db', label='Download database')

    if 'snapshot' in st.session_state:
        # dynamic categorization
        if missing_category := set(st.session_state.snapshot['asset']) - set(st.session_state.pnl_explainer.categories.keys()):
            st.warning(f"New underlyings {missing_category} -> Edit 'underlying' below to group exposures by underlying")
            with st.form("categorization_form"):
                # categorization
                st.write("Edit 'underlying' below to group exposures by underlying")
                categorization = pd.DataFrame({'underlying': {coin: coin for coin in missing_category}
                                                             | st.session_state.pnl_explainer.categories})
                categorization['exposure'] = st.session_state.snapshot.groupby('asset').sum()['value']
                edited_categorization = st.data_editor(categorization, use_container_width=True)['underlying'].to_dict()
                if st.form_submit_button("Override categorization"):
                    st.session_state.pnl_explainer.categories = edited_categorization
                    st.session_state.plex_db.overwrite_categories(edited_categorization)
                    st.session_state.plex_db.upload_to_s3()
                    st.success("Categories updated (not exposure!)")

        # display risk
        st.write("Risk pivot table: group exposures by underlying")
        st.session_state.snapshot['underlying'] = st.session_state.snapshot['asset'].map(
            st.session_state.pnl_explainer.categories)
        # risk = copy.deepcopy(st.session_state.snapshot)
        # risk['value'] = risk['value'] / 1000
        display_pivot(st.session_state.snapshot.loc[st.session_state.snapshot['value'].apply(lambda x: abs(x) > st.session_state.snapshot['value'].sum() * 1e-4)],
                      rows=['underlying', 'asset', 'chain', 'protocol'],
                      columns=['address'],
                      values=['value'],
                      hidden=['hold_mode', 'type', 'price', 'amount'])

        download_button(st.session_state.snapshot, file_name='snapshot.csv', label='Download snapshot')

with risk_history_tab:
    risk_start_timestamp, risk_end_timestamp = prompt_plex_interval(st.session_state.plex_db, addresses, nonce='risk', default_dt=timedelta(days=7))
    # snapshots
    risk_snapshots_within = st.session_state.plex_db.query_table_between(st.session_state.parameters['profile']['addresses'],
                                                                    risk_start_timestamp, risk_end_timestamp, "snapshots")
    risk_snapshots_within['timestamp'] = pd.to_datetime(risk_snapshots_within['timestamp'], unit='s', utc=True)
    risk_snapshots_within['underlying'] = risk_snapshots_within['asset'].map(
            st.session_state.pnl_explainer.categories)
    '''
    plot timeseries of explain by some staked_columns
    '''
    categoricals = ['underlying', 'asset', 'protocol', 'chain', 'hold_mode', 'type']
    values = ['value']
    rows = ['timestamp']
    granularity_field = st.selectbox("granularity field", categoricals, index=2)
    totals = pd.pivot_table(risk_snapshots_within, values=values, columns=[granularity_field], index=rows, aggfunc='sum')
    totals = totals.stack().reset_index()

    fig = px.bar(totals, x=rows[0], y=values[0],
                 color=granularity_field, title='value',
                 barmode='stack')
    min_dt = 4 * 3600  # 4h
    fig.update_traces(width=min_dt * 1000)
    st.plotly_chart(fig, use_container_width=True)

    download_button(risk_snapshots_within, file_name='risk_history.csv', label='Download risk history')

with pnl_tab:
    pnl_start_timestamp, pnl_end_timestamp = prompt_plex_interval(st.session_state.plex_db, addresses, nonce='pnl', default_dt=timedelta(days=1))

    ## display_pivot plex
    st.subheader("Pnl Explain")

    st.latex(r'PnL_{\text{full delta}} = \sum (P_{\text{asset}}^1-P_{\text{asset}}^0) \times N^{\text{start}}')
    st.latex(r'PnL_{\text{delta}} = \sum (\frac{P_{\text{underlying}}^1}{P_{\text{underlying}}^0}-1) \times N^{\text{start}} \frac{P_{\text{underlying}}^0}{P_{\text{asset}}^0}')
    st.latex(r'PnL_{\text{basis}} = PnL_{\text{full delta}} - PnL_{\text{delta}}')
    st.latex(r'PnL_{\text{amt\_chng}} = \sum \Delta N \times P^{\text{end}}')

    start_snapshot = st.session_state.plex_db.query_table_at(addresses, pnl_start_timestamp, "snapshots")
    end_snapshot = st.session_state.plex_db.query_table_at(addresses, pnl_end_timestamp, "snapshots")
    st.session_state.plex = st.session_state.pnl_explainer.explain(start_snapshot=start_snapshot, end_snapshot=end_snapshot)

    display_pivot(st.session_state.plex.loc[st.session_state.plex['pnl'].apply(lambda x: abs(x) > start_snapshot['value'].sum() * 1e-4)],
                  rows=['underlying', 'asset'],
                  columns=['pnl_bucket'],
                  values=['pnl'],
                  hidden=['protocol', 'chain', 'hold_mode', 'type'])

    download_button(st.session_state.plex, file_name='plex.csv', label='Download pnl explain')

    ## display_pivot transactions
    st.subheader("Transactions")

    transactions = st.session_state.plex_db.query_table_between(addresses, pnl_start_timestamp, pnl_end_timestamp, "transactions")
    st.session_state.transactions = st.session_state.pnl_explainer.format_transactions(pnl_start_timestamp, pnl_end_timestamp, transactions)
    st.session_state.transactions.rename(columns={'pnl': 'value'}, inplace=True)
    display_pivot(st.session_state.transactions,
                  rows=['underlying', 'asset'],
                  columns=['type'],
                  values=['gas', 'value'],
                  hidden=['id', 'protocol', 'chain'])

    download_button(st.session_state.transactions, file_name='tx.csv', label="Download tx data")

with pnl_history_tab:
    # snapshots
    pnl_snapshots_within = st.session_state.plex_db.query_table_between(st.session_state.parameters['profile']['addresses'], pnl_start_timestamp, pnl_end_timestamp, "snapshots")
    # explains and transactions btw snapshots
    explain_list = []
    transactions_list = []
    for start, end in zip(
            pnl_snapshots_within['timestamp'].unique()[:-1],
            pnl_snapshots_within['timestamp'].unique()[1:],
    ):
        start_snapshots = pnl_snapshots_within[pnl_snapshots_within['timestamp'] == start]
        end_snapshots = pnl_snapshots_within[pnl_snapshots_within['timestamp'] == end]
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

    fig = px.bar(totals, x=rows[0], y=values[0],
                 color=granularity_field, title='cum_pnl',
                 barmode='stack')
    min_dt = 4*3600 # 4h
    fig.update_traces(width=min_dt*1000)
    st.plotly_chart(fig, use_container_width=True)

    download_button(pnl_snapshots_within, file_name='snapshot.csv', label='Download pnl history')

