import asyncio
import copy
import sys
import os
from hashlib import sha256

import toml
import yaml

from plex.debank_api import DebankAPI
from utils.async_utils import safe_gather
from utils.db import PlexDB, SQLiteDB, RawDataDB, S3JsonRawDataDB

if __name__ == '__main__':
    if sys.argv[1] =='snapshot':
        with open(os.path.join(os.sep, os.getcwd(), '.streamlit', 'secrets.toml'), 'r') as f:
            secrets = toml.load(f)
        with open(os.path.join(os.sep, os.getcwd(), 'config', 'params.yaml'), 'r') as f:
            parameters = yaml.safe_load(f)

        # tamper with the db file name to add hash of debank key
        hashed_debank_key = sha256(secrets['debank'].encode()).hexdigest()[:8]
        plex_db_params = copy.deepcopy(parameters['input_data']['plex_db'])
        plex_db_params['remote_file'] = plex_db_params['remote_file'].replace('.db', f'_{hashed_debank_key}.db')

        plex_db: PlexDB = SQLiteDB(plex_db_params, secrets)
        raw_data_db: RawDataDB = RawDataDB.build_RawDataDB(parameters['input_data']['raw_data_db'], secrets)
        api = DebankAPI(raw_data_db, plex_db)

        snapshots = asyncio.run(safe_gather([api.position_snapshot(address=address, debank_key=secrets['debank'], refresh=True)
                                             for address in secrets['my_addresses']],
                                            n=min(10, len(secrets['my_addresses']))))
        plex_db.upload_to_s3()