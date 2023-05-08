
import glob
import os
from tqdm import tqdm
import pyarrow.parquet as pq
import numpy as np


def check():
    in_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards/**parquet")
    in_file_names = [os.path.basename(file_name) for file_name in in_file_paths]
    in_file_names = set(in_file_names)
    out_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/**parquet")
    out_file_names = [os.path.basename(file_name) for file_name in out_file_paths]
    out_file_names = set(out_file_names)

    exists = 0
    missing = 0

    for file_name in tqdm(in_file_paths):
        parquet_file = pq.ParquetFile(file_name)
        num_rows = parquet_file.metadata.num_rows
        num_sub_files = np.ceil(num_rows / 50_000)

        file_minus = file_name[0:-8]
        for index in range(1, num_sub_files + 1):
            sub_file_name = f"{file_minus}-sub-{index}.parquet"
            if sub_file_name in out_file_names:
                exists += 1
            else:
                missing += 1

    print("============")
    print("in files:", len(in_file_names))
    print("out files:", len(out_file_names))
    print("exists:", exists)
    print("missing:", missing)

if __name__ == "__main__":

    check()