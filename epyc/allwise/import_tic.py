import pandas as pd

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader

if __name__ == "__main__":
    type_frame = pd.read_csv("tic_types.csv")
    type_map = dict(zip(type_frame["name"], type_frame["type"]))
    args = ImportArguments(
        output_catalog_name="tic_1",
        input_path="/data3/epyc/data3/hipscat/raw/tic_csv/",
        input_format="csv.gz",
        file_reader=CsvReader(
            header=None,
            column_names=type_frame["name"].values.tolist(),
            type_map=type_map,
            chunksize=250_000,
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="ID",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        resume=True,
        highest_healpix_order=10,
        dask_n_workers=1,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
        completion_email_address="delucchi@andrew.cmu.edu",
        use_schema_file="/data3/epyc/data3/hipscat/tmp/tic_schema.parquet",
    )
    runner.pipeline(args)
