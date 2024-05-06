import glob
import os

import numpy as np
import pyarrow.parquet as pq
from tqdm import tqdm


def check():
    # in_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards/**parquet")
    in_file_paths = ["/data3/epyc/data3/hipscat/raw/ztf_shards/part-00499-shard-11.parquet"]

    out_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/**parquet")
    out_file_names = [os.path.basename(file_name) for file_name in out_file_paths]
    out_file_names = set(out_file_names)

    exists = 0
    missing = 0

    for file_path in tqdm(in_file_paths):
        parquet_file = pq.ParquetFile(file_path)
        file_name = os.path.basename(file_path)
        num_rows = parquet_file.metadata.num_rows
        print("num_rows", num_rows)

        file_minus = file_name[0:-8]
        read_rows = 0
        index = 1
        while read_rows < num_rows:
            sub_file_name = f"{file_minus}-sub-{index}.parquet"
            index += 1
            # print("sub_file_name", sub_file_name)
            if sub_file_name in out_file_names:
                exists += 1
                print("found", sub_file_name)
                read_rows += 50_000
            else:
                small = 1
                while small <= 5 and read_rows < num_rows:
                    sub_file_name = f"{file_minus}-sub-{index}-small-{small}.parquet"
                    small += 1
                    read_rows += 10_000
                    if sub_file_name in out_file_names:
                        print("found", sub_file_name)
                        exists += 1
                    else:
                        missing += 1
                        print("missing", sub_file_name)

    print("============")
    print("in files:", len(in_file_paths), "============")
    print("out files:", len(out_file_names), "============")
    print("exists:", "============", exists, "============")
    print("missing:", "============", missing, "============")


if __name__ == "__main__":
    check()
