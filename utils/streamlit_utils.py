import os
import threading
from copy import deepcopy
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import yaml
from plotly import express as px
from st_aggrid import AgGrid, GridOptionsBuilder

from utils.db import SQLiteDB


def load_parameters() -> dict:
    with open(os.path.join(os.sep, os.getcwd(), "config", 'params.yaml'), 'r') as fp:
        defaults = yaml.safe_load(fp)

    with open('temp.yaml', 'w') as file:
        yaml.dump(defaults, file)
    with open('temp.yaml', "rb") as file:
        st.sidebar.download_button(
            label="Download parameters template",
            data=file,
            file_name='temp.yaml',
            mime='yaml',
        )

    if parameter_file := st.sidebar.file_uploader("upload parameters", type=['yaml']):
        return yaml.safe_load(parameter_file)
    elif 'parameters' not in st.session_state:
        defaults['profile'] = {'debank_key': st.sidebar.text_input("debank key",
                                                                   value=st.secrets['debank_key']
                                                                   if 'debank_key' in st.secrets else '',
                                                                   help="you think i am going to pay for you?")}
        addresses = st.sidebar.text_area("addresses",
                                         help='Enter multiple strings, like a list')

        if (defaults['profile']['debank_key'] == '') or (not addresses):
            st.warning("Please enter your debank key and addresses")
            st.stop()

        defaults['profile']['addresses'] = [address for address in eval(addresses) if address[:2] == "0x"]
        return defaults
    else:
        return st.session_state.parameters


def prompt_plex_interval(plex_db: SQLiteDB, addresses: list[str]) -> tuple[int, int]:
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

    if start_datetime >= end_datetime:
        st.error("start time must be before end time")
        st.stop()

    # intersection of timestamps lists for all addresses
    list_timestamps = [plex_db.all_timestamps(address, "snapshots") for address in addresses]
    timestamps = [x for x in set(list_timestamps[0]) if all(x in lst for lst in list_timestamps)]

    start_timestamp = next((ts for ts in sorted(timestamps, reverse=True)
                            if ts <= start_datetime.timestamp()), min(timestamps))
    end_timestamp = next((ts for ts in sorted(timestamps, reverse=False)
                          if ts >= end_datetime.timestamp()), max(timestamps))

    st.write(f"Actual dates of snapshots: {datetime.fromtimestamp(start_timestamp)}, {datetime.fromtimestamp(end_timestamp)}")

    return start_timestamp, end_timestamp


def display_pivot(grid: pd.DataFrame, rows: list[str], columns: list[str], values: list[str], hidden: list[str]):
    gb = GridOptionsBuilder()
    options_dict = {
        "pivotMode": True,
        "rowSelection": 'multiple',
        "columnSelection": 'multiple',
        "suppressAggFuncInHeader": True,
        "pivotColumnGroupTotals": "after",
        "pivotRowTotals": "before",
        "enableRangeSelection": True,
        "groupIncludeTotalFooter": True,  # show total footer for each group
        "groupIncludeGroupFooter": True,  # show group footer for each group
        "groupAggFields": values,  # fields to aggregate for group footers
        "groupAggFunc": 'sum',  # aggregation function to use for group footers
    }
    gb.configure_grid_options(**options_dict)

    gb.configure_selection(selection_mode='multi')
    gb.configure_side_bar(defaultToolPanel='columns')
    gb.configure_default_column(
        resizable=True,
        filterable=True,
        sortable=True,
        editable=False,
        groupable=True
    )
    columns_defs = ({row: {'field': row, 'rowGroup': True} for row in rows}
                    | {col: {'field': col, 'pivot': True} for col in columns}
                    | {val: {'field': val, 'aggFunc': 'sum', 'type': ["numericColumn"],
                             'cellRenderer': 'agGroupCellRenderer',
                            # 'valueFormatter': lambda number: locale.currency(number, grouping=True),
                             'cellRendererParams': {'innerRenderer': 'sumRenderer'}} for val in values}
                    | {hide: {'field': hide} for hide in hidden})
    for col in columns_defs.values():
        gb.configure_column(**col)

    go = gb.build()
    grid = grid.fillna(0)
    grid[values] = grid[values].astype(int)
    grid = grid.sort_values(by=values[0], ascending=False)
    AgGrid(grid, gridOptions=go)


def download_button(df: pd.DataFrame, label: str, file_name: str, file_type='text/csv'):
    df.to_csv(file_name)
    with open(file_name, "rb") as file:
        st.download_button(
            label=label,
            data=file,
            file_name=file_name,
            mime=file_type
        )

def download_db_button(db: SQLiteDB, label: str, file_name: str, file_type='application/x-sqlite3'):
    with open(db.data_location['local_file'], "rb") as file:
        st.sidebar.download_button(
            label=label,
            data=file,
            file_name=file_name,
            mime=file_type
        )