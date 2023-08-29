import glob
import os
import re

import hipscat_import.pipeline as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table
from astropy.table.table import descr
from dask.distributed import Client
from hipscat_import.catalog.arguments import ImportArguments
from tqdm import tqdm

def convert():
    ## 094, 109, 125, 211, 240
    files = glob.glob(
        "/ocean/projects/phy210048p/shared/hipscat/raw/sdss/calibObj**.fits.gz"
    )
    files.sort()
    print("found", len(files), "fits files")

    for in_file in tqdm(files):

        match = re.match(r".*(calibObj-.*-star).fits.gz", str(in_file))
        file_prefix = match.group(1)
        out_file = f"/ocean/projects/phy210048p/shared/hipscat/raw/sdss_parquet/{file_prefix}.parquet"
        if os.path.exists(out_file):
            continue

        table = Table.read(in_file)
        new_table = Table()

        for col in table.columns.values():
            descriptor = descr(col)
            col_name = descriptor[0]
            col_shape = descriptor[2]
            if col_shape == (5,):
                data_t = col.data.T
                for index, band_char in enumerate("ugriz"):
                    new_table.add_column(data_t[index], name=f"{col_name}_{band_char}")
            elif col_shape == ():
                new_table.add_column(col)

        new_table.to_pandas().to_parquet(out_file)


if __name__ == "__main__":
    convert()
