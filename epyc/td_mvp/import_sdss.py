"""Main method to enable command line execution

"""

import glob
import tempfile

import hipscat_import.pipeline as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.table import Table
from hipscat_import.catalog.arguments import ImportArguments


def do_import():

    # files = glob.glob("/data3/epyc/data3/hipscat/raw/sdss/**.fits.gz")
    # files.sort()
    args = ImportArguments(
        output_catalog_name="sdss_dr16q",
        # input_file_list=files,
        # file_reader=FitsReader(
        #     chunksize=100_000,
        #     skip_column_names=MULTIDIMENSIONAL,
        # ),
        input_path="/data3/epyc/data3/hipscat/raw/sdss/parquet/",
        input_format="parquet",
        ra_column="RA",
        dec_column="DEC",
        id_column="ID",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        # overwrite=True,
        # resume=True,
        highest_healpix_order=7,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
    )
    runner.pipeline(args)


def read_fits():
    """Make sure we can read the file at all."""
    filename = "/data3/epyc/data3/hipscat/raw/sdss/calibObj-008162-4-star.fits.gz"
    chunksize = 50_000

    table = Table.read(filename, memmap=True)
    table.remove_columns(MULTIDIMENSIONAL)

    total_rows = len(table)
    read_rows = 0

    while read_rows < total_rows:
        print(len(table[read_rows : read_rows + chunksize].to_pandas()))
        read_rows += chunksize
        print(read_rows)


if __name__ == "__main__":
    do_import()
    # read_fits()
