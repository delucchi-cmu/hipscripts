import glob
import re

import pandas as pd
from astropy.table import Table
from astropy.table.table import descr
from tqdm import tqdm


def convert():
    base_dir = "/home/delucchi/td_data/agns/"

    table = Table.read(os.path.join(base_dir, "dr16q_prop_May16_2023.fits.gz"), memmap=True)

    total_rows = len(table)
    print("total_rows", total_rows)
    read_rows = 0
    chunksize = 100_000
    chunk_num = 0

    while read_rows < total_rows:
        subtable = table[read_rows : read_rows + chunksize]
        read_rows += chunksize

        new_table = Table()
        for col in subtable.columns.values():
            descriptor = descr(col)
            col_name = descriptor[0]
            col_shape = descriptor[2]
            if col_shape == ():
                # print("scalar:", col_name)
                new_table.add_column(col)
            else:
                # print("multi-column", col_name)
                data_t = col.data.T
                for index in range(col_shape[0]):
                    arr_name = f"{col_name}_arr{index}"
                    # print("    ", arr_name)
                    new_table.add_column(data_t[index], name=arr_name)

        new_table.to_pandas().to_parquet(os.path.join(base_dir, f"dr16q_prop_May16_2023_{chunk_num}.parquet"))
        print("wrote chunk", chunk_num)
        chunk_num += 1


if __name__ == "__main__":
    convert()
