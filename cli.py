import asyncio
import sys
import toml

from plex.debank_api import DebankAPI
from utils.async_utils import safe_gather
from utils.db import CsvDB

if __name__ == '__main__':
    if sys.argv[1] =='snapshot':
        db = CsvDB()
        api = DebankAPI(db)
        with open(sys.argv[2], 'r') as f:
            secrets = toml.load(f)
        snapshots = asyncio.run(safe_gather([api.position_snapshot(address=address, debank_key=secrets['debank'], refresh=True)
                                             for address in secrets['my_addresses']],
                                            n=min(10, len(secrets['my_addresses']))))