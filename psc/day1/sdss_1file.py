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


def do_import():
    local_tmp = os.path.expandvars("$LOCAL")
    args = ImportArguments(
        output_catalog_name="sdss_1file",
        input_path="/ocean/projects/phy210048p/shared/hipscat/raw/sdss_parquet/",
        input_format="parquet",
        ra_column="RA",
        dec_column="DEC",
        id_column="ID",
        pixel_threshold=1_000_000,
        tmp_dir=local_tmp,
        overwrite=True,
        resume=True,
        highest_healpix_order=7,
        dask_n_workers=1,
        dask_threads_per_worker=1,
        dask_tmp=local_tmp,
        debug_stats_only=True,
        completion_email_address="delucchi@andrew.cmu.edu",
        output_path="/ocean/projects/phy210048p/shared/hipscat/test_catalogs/",
    )

    with Client(
        local_directory=args.dask_tmp,
        n_workers=args.dask_n_workers,
        threads_per_worker=args.dask_threads_per_worker,
        memory_limit=None,
    ) as client:
        runner.pipeline_with_client(args, client)


def convert():
    files = glob.glob(
        "/ocean/projects/phy210048p/shared/hipscat/raw/sdss/calibObj-000094**.fits.gz"
    )
    files.sort()

    for in_file in tqdm(files, bar_format="{l_bar}{bar:80}{r_bar}"):

        match = re.match(r".*(calibObj-.*-star).fits.gz", str(in_file))
        file_prefix = match.group(1)
        out_file = f"/ocean/projects/phy210048p/shared/hipscat/raw/sdss_parquet/{file_prefix}.parquet"

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
    # convert()
    do_import()
    # read_fits()
