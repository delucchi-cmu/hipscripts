import pandas as pd

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments


def do_import():
    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/catalogs/",
        output_artifact_name="dp02",
        input_path="/data3/epyc/data3/hipscat/raw/dp02",
        # input_file_list=["/data3/epyc/data3/hipscat/raw/dp02/objectTable_tract_3447_DC2_2_2i_runs_DP0_2_v23_0_1_PREOPS-905_step3_9_20220302T181001Z.parq"],
        file_reader="parquet",
        sort_columns="objectId",
        ra_column="coord_ra",
        dec_column="coord_dec",
        pixel_threshold=300_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        highest_healpix_order=8,
        dask_n_workers=20,
        dask_threads_per_worker=1,
        use_schema_file="/astro/users/mmd11/git/scripts/epyc/dp0/metadata.parquet",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    do_import()