# https://actualyield.streamlit.app/

This Debank powered Streamlit app allows on-chain yield farmers to visualize their risk and understand what drives their pnl.

More details on 

## Architecture
### 1) Data collection (plex/debank_api)
Leverages Debank API to decompose risk across all protocols, wallet holdings and nft.
### 2) Data storage (utils/db.py)
Raw data is stored on S3, and derived data is compiled into 'snapshot', 'transactions' and 'categories' SQLite databases. 

Those files live on S3 and are unique to each user (ie. to each debank key). Please note: concurrent usage of a single debank key is not unsafe.
### 3) plex computations (plex/plex.py)
Performs pnl explain btw 2 snapshots, also displays all transactions.

One key feature is the ability to group tokens by underlying (eg ETH-pegged, USDC-pegged...) to separate impact of majors moves from basis moves (eg fluctuations from peg, yield accrual..)

This is driven by the user through the categorization feature.
### 5) configs (config/params.yaml)
mostly S3 paths
### 6) streamlit UI (./pnl_explain.py)
- enter debank key and addresses on the sidebar
- can trigger a live snapshot and store it
- displays either live or historical risk
- displays granular pnl explain + transactions btw 2 dates
- displays historical pnl explain stacked bars
### 7) headless snapshot script (./cli.py)
This is meant to be run as a cron job to regularly fetch data from debank to S3.
# guide
- to install, run `pip install -r requirements.txt`
- then run module streamlit `run pnl_explain.py` to launch the streamlit app