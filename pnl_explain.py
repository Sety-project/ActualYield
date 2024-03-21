import asyncio
import html
import os
import sys
from datetime import timedelta, date, datetime, timezone, time

import matplotlib
import pandas as pd
import streamlit as st
import yaml

from plex.plex import PnlExplainer
from utils.async_utils import safe_gather
from utils.db import CsvDB
from plex.debank_api import DebankAPI

assert (sys.version_info >= (3, 10)), "Please use Python 3.10 or higher"

if 'stage' not in st.session_state:
    st.set_page_config(layout="wide")
    st.session_state.db = CsvDB()
    st.session_state.api = DebankAPI(st.session_state.db)
    st.session_state.pnl_explainer = PnlExplainer()
    st.session_state.stage = 0

from utils.streamlit_utils import authentification_sidebar, load_parameters, \
    prompt_plex_interval, display_pivot

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()
authentification_sidebar()

snapshot_tab, risk_tab, plex_tab = st.tabs(
    ["snapshot", "risk", "pnl explain"])

if 'my_addresses' not in st.secrets:
    addresses = st.sidebar.text_area("addresses", help='Enter multiple strings on separate lines').split('\n')
else:
    addresses = st.secrets['my_addresses']
addresses = [address for address in addresses if address[:2] == "0x"]

with snapshot_tab:
    with st.form("snapshot_form"):
        refresh = st.checkbox("fetch from debank", value=False, help="fetch from debank costs credits !")
        if st.form_submit_button("get latest snapshot", help="either from debank or from db"):
            if st.session_state.authentification == 'verified':
                with st.spinner(f"Taking snapshots"):
                    debank_key = st.text_input("debank key",
                                                  value=st.secrets['debank'] if 'debank' in st.secrets else '',
                                                  help="you think i am going to pay for you?")

                    snapshots = asyncio.run(safe_gather([st.session_state.api.position_snapshot(address,
                                                                                                 debank_key,
                                                                                                 refresh=refresh)
                                                         for address in addresses],
                                                        n=st.session_state.parameters['input_data']['async']['gather_limit']))
                    snapshot = pd.concat(snapshots, axis=0, ignore_index=True)
                    st.dataframe(snapshot)
                    st.session_state.snapshot = snapshot
            else:
                st.warning(
                    html.unescape(
                        'chat https://t.me/Pronoia_Bot, then enter your tg handle in the sidebar to get access'
                    )
                )

    if 'snapshot' in st.session_state:
        st.session_state.snapshot.to_csv('temp.csv')
        with open('temp.csv', "rb") as file:
            st.download_button(
                label="Download risk data",
                data=file,
                file_name='temp.csv',
                mime='text/csv',
            )

with risk_tab:
    # dynamic categorization
    with st.form("categorization_form"):
        if 'snapshot' in st.session_state:
            if missing_category := set(st.session_state.snapshot['asset']) - set(st.session_state.pnl_explainer.categories.keys()):
                st.warning(f"New underlyings {missing_category} -> Edit 'underlying' below to group exposures by underlying")

            # display risk
            st.write("Risk pivot table: group exposures by underlying")
            st.session_state.snapshot['underlying'] = st.session_state.snapshot['asset'].map(
                st.session_state.pnl_explainer.categories)
            display_pivot(st.session_state.snapshot,
                          rows=['underlying', 'asset', 'chain', 'protocol'],
                          columns=['address'],
                          values=['value', 'price', 'amount'],
                          hidden=['hold_mode', 'type'])

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
                st.session_state.stage = 1

with plex_tab:
    start_datetime, end_datetime = prompt_plex_interval()

    # fetch data from db
    start_snapshots = {'snapshot': {}, 'timestamp': {}}
    end_snapshots = {'snapshot': {}, 'timestamp': {}}
    for address in addresses:
        data = st.session_state.db.query_explain_data(address, start_datetime, end_datetime)
        start_snapshots['snapshot'][address] = st.session_state.api.parse_snapshot(data['start_snapshot'])
        start_snapshots['timestamp'][address] = datetime.fromtimestamp(data['start_snapshot']['timestamp'], tz=timezone.utc)
        end_snapshots['snapshot'][address] = st.session_state.api.parse_snapshot(data['end_snapshot'])
        end_snapshots['timestamp'][address] = datetime.fromtimestamp(data['end_snapshot']['timestamp'], tz=timezone.utc)
    start_snapshot = pd.concat(start_snapshots['snapshot'].values(), axis=0, ignore_index=True)
    end_snapshot = pd.concat(end_snapshots['snapshot'].values(), axis=0, ignore_index=True)
    st.write("Actual dates of snapshots:")
    st.dataframe(pd.concat([pd.Series(x['timestamp']) for x in [start_snapshots, end_snapshots]], axis=1, ignore_index=True))

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

