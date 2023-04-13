import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

if __name__ == "__main__":
    s = time.time()

    parser = argparse.ArgumentParser(
        prog="LSD2 Partitioner",
        description="Instantiate a partitioned catalog from unpartitioned sources",
    )
    parser.add_argument(
        "hipscat_dir",
        help="path to hipscat catalog",
    )
    parser.add_argument("id_column", default="diaObjectId")
    args = parser.parse_args(sys.argv[1:])
    hipscat_dir = args.hipscat_dir
    id_column = args.id_column

    partition_info = os.path.join(hipscat_dir, "partition_info.csv")
    data_frame = pd.read_csv(partition_info)

    output_frame = data_frame.copy()
    output_frame["num_rows"] = np.nan
    output_frame["num_ids"] = np.nan
    output_frame["size_on_disk"] = 0

    for index, partition in data_frame.iterrows():
        file_name = os.path.join(
            hipscat_dir,
            f"Norder{partition['order']}",
            f"Npix{partition['pixel']}",
            "catalog.parquet",
        )
        output_frame.loc[index, "size_on_disk"] = os.path.getsize(file_name)

        parquet_file = pq.ParquetFile(file_name)
        output_frame.loc[index, "num_rows"] = parquet_file.metadata.num_rows
        # print(parquet_file.metadata.num_rows)

        partition_frame = pd.read_parquet(file_name, engine="pyarrow")

        assert id_column in partition_frame.columns
        ids = partition_frame[id_column].tolist()
        set_ids = [*set(ids)]
        output_frame.loc[index, "num_ids"] = len(set_ids)

    print(output_frame.head(2))

    output_frame = output_frame.astype(int)
    partition_info = os.path.join(hipscat_dir, "more_partition_info.csv")
    output_frame.to_csv(partition_info, index=False)

    e = time.time()
    print(f"Elapsed Time: {e-s}")
