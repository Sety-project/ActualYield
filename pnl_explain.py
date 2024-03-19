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

from utils.streamlit_utils import authentification_sidebar, load_parameters, prompt_initialization, \
    prompt_plex_interval, display_risk_pivot

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

                    async def position_snapshot(address: str) -> pd.DataFrame:
                        '''
                        fetch from debank if not recently updated, or db if recently updated or refresh=False
                        then write to json to disk
                        returns parsed latest snapshot summed across all addresses
                        only update once every 'update_frequency' minutes
                        '''
                        last_update = st.session_state.db.last_updated(address)
                        updated_at = last_update[0]
                        snapshot = st.session_state.api.parse_snapshot(last_update[1])

                        # retrieve cache for addresses that have been updated recently, and always if refresh=False
                        max_updated = datetime.now(tz=timezone.utc) - timedelta(
                            minutes=st.session_state.parameters['plex']['update_frequency'])
                        if refresh:
                            if updated_at < max_updated and refresh:
                                snapshot_dict = await st.session_state.api.fetch_position_snapshot(address, debank_key, write_to_json=True)
                                snapshot = st.session_state.api.parse_snapshot(snapshot_dict)
                            else:
                                st.warning(f"We only update once every {st.session_state.parameters['plex']['update_frequency']} minutes. {address} not refreshed")
                        snapshot['address'] = address
                        return snapshot

                    snapshots = asyncio.run(safe_gather([position_snapshot(address) for address in addresses],
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

with risk_tab:
    # dynamic categorization
    with st.form("categorization_form"):
        if 'snapshot' in st.session_state:
            st.write("Risk pivot table: group exposures by underlying")
            st.session_state.snapshot['underlying'] = st.session_state.snapshot['asset'].map(
                st.session_state.pnl_explainer.categories)
            display_risk_pivot(st.session_state.snapshot)

            st.write("Edit 'underlying' below to group exposures by underlying")
            categorization = pd.DataFrame({'underlying': {coin: coin
                                           for coin in st.session_state.snapshot['asset'].unique()
                                           if coin not in st.session_state.pnl_explainer.categories}
                                          | st.session_state.pnl_explainer.categories})
            categorization['exposure'] = st.session_state.snapshot.groupby('asset').sum()['value']
            edited_categorization = st.data_editor(categorization, use_container_width=True)['underlying'].to_dict()
            if st.form_submit_button(f"Override categorization"):
                st.session_state.pnl_explainer.categories = edited_categorization
                with open(st.session_state.pnl_explainer.categories_path, 'w') as f:
                    yaml.dump(edited_categorization, f)
                st.success("Categories updated (not exposure!)")
                st.session_state.stage = 1

with plex_tab:
    start_datetime, end_datetime = prompt_plex_interval()
    if 'snapshot' in st.session_state:
        results = {}
        for address in addresses:
            data = st.session_state.db.query_explain_data(address, start_datetime, end_datetime)
            start_snapshot = st.session_state.api.parse_snapshot(data['start_snapshot'])
            end_snapshot = st.session_state.api.parse_snapshot(data['end_snapshot'])
            results[address] = st.session_state.pnl_explainer.explain(start_snapshot=start_snapshot, end_snapshot=end_snapshot)
            st.dataframe(results[address][0])
            st.dataframe(results[address][1])
            st.dataframe(results[address][2])

