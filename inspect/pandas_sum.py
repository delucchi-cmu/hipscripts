import argparse
import sys
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

if __name__ == "__main__":
    s = time.time()

    file_name = "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/association/intermediate/partitions.csv"
    sum_column = "primary_id"

    data_frame = pd.read_csv(file_name)

    assert sum_column in data_frame.columns
    print("sum:", data_frame[sum_column].sum())
    ids = data_frame[sum_column].tolist()
    print(len(ids))

    unique_id, count_id = np.unique(ids, return_counts=True)
    print(unique_id)
    print(count_id)
    # set_ids = [*set(ids)]
    # print(len(set_ids))
    # print(set_ids)

    e = time.time()
    print(f"Elapsed Time: {e-s}")
