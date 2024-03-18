import json
import os

import pandas as pd
from datetime import datetime

class PnlExplainer:
    def __init__(self, snapshot_start: pd.DataFrame, snapshot_end: pd.DataFrame, transactions: pd.DataFrame = pd.DataFrame()):
        self.snapshot_start: pd.Series = snapshot_start.set_index([col for col in snapshot_start.columns if col not in ['amount', 'value']]).squeeze()
        self.snapshot_end: pd.Series = snapshot_end.set_index([col for col in snapshot_end.columns if col not in ['amount', 'value']]).squeeze()
        self.transactions = transactions

    def explain(self):
        common_positions = self.snapshot_start.index.intersection(self.snapshot_end.index)
        common = self.snapshot_end[common_positions] - self.snapshot_start[common_positions]
        added = self.snapshot_end[~self.snapshot_end.index.isin(self.snapshot_start.index)]
        removed = self.snapshot_start[~self.snapshot_start.index.isin(self.snapshot_end.index)]