import pandas as pd

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader

import hipscat
import os
import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader
import pyarrow.parquet as pq
import pyarrow as pa
import re
import glob

class DirectoryParquetReader(ParquetReader):
    def read(self, input_file):
        """Reader for the specifics of zubercal parquet files."""
        keep_columns=["index",
                "ps1_objid",
                "ra",
                "dec",
                "ps1_gMeanPSFMag",
                "ps1_rMeanPSFMag",
                "ps1_iMeanPSFMag",
                "nobs_g",
                "nobs_r",
                "nobs_i",
                "mean_mag_g",
                "mean_mag_r",
                "mean_mag_i",
                "catflags",
                "fieldID",
                "mag",
                "magerr",
                "mjd",
                "rcID",
                "band",
                ]


        ## Find all the parquet files in the directory
        files = glob.glob(f"{input_file}/**.parquet")
        files.sort()

        for file in files:
            yield pd.read_parquet(file, columns=keep_columns)

def do_import():

    catalog_dir = "/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_source/"
    catalog = hipscat.read_from_hipscat(catalog_dir)
    info_frame = catalog.partition_info.as_dataframe()
    groups = info_frame.groupby(["Norder", "Dir"])
    paths = [os.path.join(catalog_dir, f"Norder={group_index[0]}", f"Dir={group_index[1]}") for group_index, _ in groups]

    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/test_catalogs/ztf_axs/",
        output_artifact_name="ztf_zource",
        input_file_list=paths[0:3],
        input_format="parquet",
        file_reader=DirectoryParquetReader(),
        ra_column="ra",
        dec_column="dec",
        sort_columns="ps1_objid",
        pixel_threshold=30_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        highest_healpix_order=9,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    do_import()