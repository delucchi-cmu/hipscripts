"""Main method to enable command line execution"""

import pandas as pd

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader

if __name__ == "__main__":
    type_frame = pd.read_csv("types.csv", keep_default_na=False)
    type_map = dict(zip(type_frame["name"], type_frame["type"]))
    args = ImportArguments(
        output_catalog_name="neowise_yr8",
        input_path="/epyc/data3/NEOWISE-yr8/irsa.ipac.caltech.edu/data/download/neowiser_year8/",
        input_format="csv.bz2",
        file_reader=CsvReader(
            header=None,
            separator="|",
            column_names=type_frame["name"].values.tolist(),
            type_map=type_map,
            chunksize=250_000,
        ),
        ra_column="RA",
        dec_column="DEC",
        id_column="SOURCE_ID",
        pixel_threshold=2_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        highest_healpix_order=9,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
        completion_email_address="delucchi@andrew.cmu.edu",
        resume=True,
        overwrite=True,
        use_schema_file="/data3/epyc/data3/hipscat/tmp/neowise_schema.parquet",
    )
    runner.pipeline(args)
