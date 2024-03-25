import asyncio
import copy
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
from utils.db import SQLiteDB, RawDataDB
from plex.debank_api import DebankAPI

assert (sys.version_info >= (3, 10)), "Please use Python 3.10 or higher"

if 'db' not in st.session_state:
    st.set_page_config(layout="wide")
    st.session_state.plex_db = SQLiteDB()
    st.session_state.api = DebankAPI(RawDataDB(), st.session_state.plex_db)
    st.session_state.pnl_explainer = PnlExplainer()

from utils.streamlit_utils import authentification_sidebar, load_parameters, \
    prompt_plex_interval, display_pivot

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()
# authentification_sidebar()
st.session_state['authentification'] = 'verified'

risk_tab, plex_details_tab, plex_history_tab = st.tabs(
    ["risk", "pnl_details", "pnl_history"])

if 'my_addresses' not in st.secrets:
    addresses = st.sidebar.text_area("addresses", help='Enter multiple strings on separate lines').split('\n')
else:
    addresses = st.secrets['my_addresses']
addresses = [address for address in addresses if address[:2] == "0x"]

with st.sidebar.form("snapshot_form"):
    if st.session_state.authentification == 'verified':
        if refresh := st.form_submit_button("fetch from debank", help="fetch from debank costs credits !"):
            debank_key = st.sidebar.text_input("debank key",
                                          value=st.secrets['debank'] if 'debank' in st.secrets else '',
                                          help="you think i am going to pay for you?")
        else:
            debank_key = None

        snapshots = asyncio.run(safe_gather([st.session_state.api.position_snapshot(address,
                                                                                     debank_key,
                                                                                     refresh=refresh)
                                             for address in addresses],
                                            n=st.session_state.parameters['input_data']['async']['gather_limit']))
        snapshot = pd.concat(snapshots, axis=0, ignore_index=True)
        st.session_state.snapshot = snapshot
    else:
        st.sidebar.warning(
            html.unescape(
                'chat https://t.me/Pronoia_Bot, then enter your tg handle in the sidebar to get access'
            )
        )

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

with plex_details_tab:
    start_datetime, end_datetime = prompt_plex_interval()

    # fetch data from db
    start_list = {}
    end_list = {}
    for address in addresses:
        data = st.session_state.plex_db.query_explain_data(address, start_datetime, end_datetime)
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

