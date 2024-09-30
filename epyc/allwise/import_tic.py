import glob

import hipscat_import.pipeline as runner
import pandas as pd
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader
from csv_gz_reader import CsvGzReader
import hipscat_import.pipeline as runner
from hipscat_import.margin_cache.margin_cache_arguments import MarginCacheArguments

import ray
from ray.util.dask import enable_dask_on_ray, disable_dask_on_ray
from dask.distributed import Client
import dask

if __name__ == "__main__":
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/tic/tic",
        output_path="/data3/epyc/data3/hipscat/catalogs/tic",
        output_artifact_name="tic_10arcs",
        margin_threshold=10,
        dask_n_workers=20,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)



def import_catalog():
    files = glob.glob("/data3/epyc/data3/hipscat/raw/tic_csv/*.csv.gz")
    files.sort()
    print(f"found {len(files)} num files")
    
    args = ImportArguments(
        output_artifact_name="tic",
        input_file_list=files,
        file_reader=CsvGzReader(
            header=None,
            schema_file="/data3/epyc/data3/hipscat/tmp/tic_schema.parquet",
            chunksize=250_000,
        ),
        ra_column="ra",
        dec_column="dec",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        highest_healpix_order=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/tic/",
        completion_email_address="delucchi@andrew.cmu.edu",
        use_schema_file="/data3/epyc/data3/hipscat/tmp/tic_schema.parquet",
    )
    runner.pipeline(args)
