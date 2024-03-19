import os
import pandas as pd
import yaml


class PnlExplainer:
    def __init__(self):
        self.categories_path = os.path.join(os.getcwd(), 'config', 'categories.yaml')
        with open(self.categories_path, 'r') as f:
            self.categories = yaml.safe_load(f)

    def explain(self, start_snapshot: pd.DataFrame, end_snapshot: pd.DataFrame, transactions: pd.DataFrame = pd.DataFrame()):
        self.snapshot_start = start_snapshot.set_index([col for col in start_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        self.snapshot_end = end_snapshot.set_index([col for col in end_snapshot.columns if col not in ['price', 'amount', 'value', 'timestamp']])
        self.transactions = transactions

        common_positions = self.snapshot_start.index.intersection(self.snapshot_end.index)
        common = self.snapshot_end.loc[common_positions] - self.snapshot_start.loc[common_positions]
        added = self.snapshot_end[~self.snapshot_end.index.isin(self.snapshot_start.index)]
        removed = self.snapshot_start[~self.snapshot_start.index.isin(self.snapshot_end.index)]

        return common, added, removed