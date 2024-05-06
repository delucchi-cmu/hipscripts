import pandas as pd

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader


def do_import_5():
    file_names = [
        "EpochPhotometry_766858-767082.csv.gz",
"EpochPhotometry_767083-767466.csv.gz",
"EpochPhotometry_767467-767715.csv.gz",
"EpochPhotometry_767716-768009.csv.gz",
"EpochPhotometry_768010-768122.csv.gz",
    ]


    input_file_list=[
        f"/data3/epyc/data3/hipscat/raw/gaia/epoch_photometry/{file_name}"
        for file_name in file_names
    ]

    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="gaia_photometry",
        input_file_list=input_file_list,
        file_reader=CsvReader(
            comment="#",
        ),
        ra_column="ra",
        dec_column="dec",
        sort_columns="source_id",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        highest_healpix_order=8,
        dask_n_workers=10,
        resume=True,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

def do_import():
    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3",
        output_artifact_name="gaia",
        input_path="/data3/epyc/data3/hipscat/raw/gaia/",
        input_format="csv.gz",
        file_reader=CsvReader(
            comment="#",
            schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        ),
        ra_column="ra",
        dec_column="dec",
        sort_columns="solution_id",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        use_schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        highest_healpix_order=8,
        dask_n_workers=40,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    do_import()