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

from utils.streamlit_utils import load_parameters, prompt_plex_interval, display_pivot

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()

st.session_state.debank_key = st.sidebar.text_input("debank key",
                                   value=st.secrets['debank'] if 'debank' in st.secrets else '',
                                   help="you think i am going to pay for you?")
# tamper with the db file name to add hash of debank key
hashed_debank_key = st.session_state.debank_key
plex_db_params = copy.deepcopy(st.session_state.parameters['input_data']['plex_db'])
plex_db_params['remote_file'] = plex_db_params['remote_file'].replace('.db', f'_{hashed_debank_key}.db')

# TODO: no pb reloading each time ? bc of sql
if 'plex_db' not in st.session_state:
    st.session_state.plex_db: PlexDB = SQLiteDB(plex_db_params, st.secrets)
    raw_data_db: RawDataDB = RawDataDB.build_RawDataDB(st.session_state.parameters['input_data']['raw_data_db'], st.secrets)
    st.session_state.api = DebankAPI(raw_data_db, st.session_state.plex_db)
    st.session_state.pnl_explainer = PnlExplainer()

risk_tab, pnl_tab = st.tabs(
    ["risk", "pnl"])

if 'my_addresses' not in st.secrets:
    addresses = st.sidebar.text_area("addresses", help='Enter multiple strings on separate lines').split('\n')
else:
    addresses = st.secrets['my_addresses']
addresses = [address for address in addresses if address[:2] == "0x"]

with st.sidebar.form("snapshot_form"):
    refresh = st.form_submit_button("fetch from debank", help="fetch from debank costs credits !")

    snapshots = asyncio.run(safe_gather([st.session_state.api.position_snapshot(address, debank_key=st.session_state.debank_key, refresh=refresh)
                                         for address in addresses],
                                        n=st.session_state.parameters['run_parameters']['async']['gather_limit']))
    if refresh:
        st.session_state.plex_db.upload_to_s3()
    snapshot = pd.concat(snapshots, axis=0, ignore_index=True)
    st.session_state.snapshot = snapshot

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

        st.session_state.snapshot.to_csv('temp.csv')
        with open('temp.csv', "rb") as file:
            st.download_button(
                label="Download risk data",
                data=file,
                file_name='temp.csv',
                mime='text/csv',
            )

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
    start_datetime, end_datetime = prompt_plex_interval()

    details_tab, history_tab = st.tabs(["details", "history"])

    with details_tab:
        # fetch data from db
        start_list = {}
        end_list = {}
        for address in addresses:
            data = st.session_state.plex_db.query_start_end_snapshots(address, start_datetime, end_datetime)
            start_list[address] = data['start_snapshot']
            end_list[address] = data['end_snapshot']
        start_snapshot = pd.concat(start_list.values(), axis=0, ignore_index=True)
        end_snapshot = pd.concat(end_list.values(), axis=0, ignore_index=True)
        st.write("Actual dates of snapshots:")
        st.dataframe(pd.concat([pd.Series({address: datetime.fromtimestamp(df['timestamp'].iloc[0], tz=timezone.utc) for address, df in x.items()})
                                          for x in [start_list, end_list]], axis=1, ignore_index=True))

        # perform pnl explain
        st.latex(r'PnL_{\text{delta}} = \sum \Delta P_{\text{underlying}} \times N^{\text{start}}')
        st.latex(r'PnL_{\text{basis}} = \sum \Delta (P_{\text{asset}}-P_{\text{underlying}}) \times N^{\text{start}}')
        st.latex(r'PnL_{\text{amt\_chng}} = \sum \Delta N \times P^{\text{end}}')
        st.session_state.plex = st.session_state.pnl_explainer.explain(start_snapshot=start_snapshot, end_snapshot=end_snapshot)
        display_pivot(st.session_state.plex,
                      rows=['underlying', 'asset'],
                      columns=['pnl_bucket'],
                      values=['pnl'],
                      hidden=['protocol', 'chain', 'hold_mode', 'type'])

        if 'plex' in st.session_state:
            st.session_state.plex.to_csv('temp.csv')
            with open('temp.csv', "rb") as file:
                st.download_button(
                    label="Download plex data",
                    data=file,
                    file_name='temp.csv',
                    mime='text/csv',
                )

    with history_tab:
        # snapshots
        snapshots_within = pd.concat([st.session_state.plex_db.query_snapshots_within(address, start_datetime, end_datetime)
                                      for address in addresses], axis=0, ignore_index=True)
        # explains btw snapshots
        explains = []
        for start, end in zip(
                snapshots_within['timestamp'].unique()[:-1],
                snapshots_within['timestamp'].unique()[1:],
        ):
            start_snapshots = snapshots_within[snapshots_within['timestamp'] == start]
            end_snapshots = snapshots_within[snapshots_within['timestamp'] == end]
            explains.append(st.session_state.pnl_explainer.explain(start_snapshots, end_snapshots))
        explains = pd.concat(explains, axis=0, ignore_index=True)

        # plot timeseries of pnl by some staked_columns
        categoricals = ['underlying', 'asset', 'protocol', 'pnl_bucket', 'chain', 'hold_mode', 'type']
        values = ['pnl']
        rows = ['timestamp_end']
        granularity_field = st.selectbox("granularity field", categoricals, index=0)
        relevant_columns = categoricals + values + rows
        totals = pd.pivot_table(explains, values=values, columns=[granularity_field], index=rows, aggfunc='sum').cumsum()
        totals = totals.stack().reset_index()

        fig = px.bar(totals, x='timestamp_end', y='pnl',
                     color=granularity_field, title='cum_pnl',
                     barmode='stack')
        min_dt = 4*3600 # 4h
        fig.update_traces(width=min_dt*1000)
        st.plotly_chart(fig, use_container_width=True)

        if 'history' in st.session_state:
            st.session_state.history.to_csv('temp.csv')
            with open('temp.csv', "rb") as file:
                st.download_button(
                    label="Download history data",
                    data=file,
                    file_name='temp.csv',
                    mime='text/csv',
                )

