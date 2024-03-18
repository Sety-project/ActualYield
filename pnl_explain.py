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
from utils.streamlit_utils import authentification_sidebar, load_parameters

assert (sys.version_info >= (3, 10)), "Please use Python 3.10 or higher"

pd.options.mode.chained_assignment = None
st.session_state.parameters = load_parameters()

st.session_state.database = CsvDB()
authentification_sidebar()

snapshot_tab, risk_tab, plex_tab = st.tabs(
    ["snapshot", "risk", "pnl explain"])

if 'stage' not in st.session_state:
    st.session_state.stage = 0
    st.db = CsvDB()

if 'my_addresses' not in st.secrets:
    addresses = st.sidebar.text_area("addresses", help='Enter multiple strings on separate lines').split('\n')
else:
    addresses = st.secrets['my_addresses']
addresses = [address for address in addresses if address[:2] == "0x"]

with snapshot_tab:
    with st.form("snapshot_form"):
        if st.form_submit_button("take snapshot"):
            if st.session_state.authentification == 'verified':
                with st.spinner(f"Taking snapshots"):
                    debank_key = st.text_input("debank key",
                                                  value=st.secrets['debank'] if 'debank' in st.secrets else '',
                                                  help="you think i am going to pay for you?")
                    obj = DebankAPI(debank_key)

                    async def position_snapshots() -> dict[str, pd.DataFrame]:
                        '''
                        fetches from debank and write to json to disk
                        returns parsed latest snapshot as a dict[address,DataFrames]
                        only update once every 'update_frequency' minutes'''
                        last_updated = await safe_gather(
                            [st.db.last_updated(address)
                              for address in addresses],
                            n=st.session_state.parameters['input_data']['async']['gather_limit'])
                        max_updated = datetime.now(tz=timezone.utc) - timedelta(minutes=st.session_state.parameters['plex']['update_frequency'])

                        # retrieve cache for addresses that have been updated recently
                        cached_snapshots = {address: obj.parse_snapshot(last_update[1])
                                            for address, last_update in zip(addresses, last_updated)
                                            if last_update[0] >= max_updated}

                        for address in cached_snapshots:
                            st.warning(f"We only update once every {st.session_state.parameters['plex']['update_frequency']} minutes. {address} not refreshed")

                        # fetch for addresses that need to be refreshed
                        addresses_to_refresh = [address
                                                for address, last_update in zip(addresses, last_updated)
                                                if last_update[0] < max_updated]

                        if not addresses_to_refresh:
                            return cached_snapshots

                        # fetch snapshots
                        json_results = await safe_gather(
                            [st.fetch_position_snapshot(address)
                              for address in addresses_to_refresh],
                            n=st.session_state.parameters['input_data']['async']['gather_limit'])
                        refreshed_snapshots = {address: obj.parse_snapshot(refreshed_snapshots)
                                            for address, refreshed_snapshots in zip(addresses_to_refresh, json_results)}

                        return cached_snapshots | refreshed_snapshots

                    snapshot_by_address = asyncio.run(position_snapshots())
                    for address, snapshot in snapshot_by_address.items():
                        st.write(address)
                        st.dataframe(snapshot)
                    st.session_state.snapshots = snapshot_by_address
                    st.session_state.stage = 1
            else:
                st.warning(
                    html.unescape(
                        'chat https://t.me/Pronoia_Bot, then enter your tg handle in the sidebar to get access'
                    )
                )

with risk_tab:
    snapshot = None

with plex_tab:
    date_col, time_col = st.columns(2)
    now_datetime = datetime.now()
    with time_col:
        start_time = st.time_input("start time", value=now_datetime.time())
        end_time = st.time_input("end time", value=now_datetime.time())
    with date_col:
        start_date = st.date_input("start date", value=now_datetime - timedelta(days=1))
        end_date = st.date_input("end date", value=now_datetime)
    start_datetime = datetime.combine(start_date, start_time)
    end_datetime = datetime.combine(end_date, end_time)

    for address in addresses:
        args = asyncio.run(st.db.query_explain_data(address, start_datetime, end_datetime))
        PnlExplainer().explain(*args)
