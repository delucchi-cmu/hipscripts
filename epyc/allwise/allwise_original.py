"""Main method to enable command line execution
"""

import glob

import hipscat_import.pipeline as runner
import pandas as pd
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader

if __name__ == "__main__":

    # Load the column names and types from a side file.
    type_frame = pd.read_csv("allwise_types.csv")
    type_map = dict(zip(type_frame["name"], type_frame["type"]))

    files = glob.glob("/data3/epyc/data3/hipscat/raw/allwise_raw/wise-allwise-cat-part**")
    files.sort()

    args = ImportArguments(
        output_catalog_name="allwise",
        input_file_list=files,
        input_format="csv",
        file_reader=CsvReader(
            header=None,
            separator="|",
            column_names=type_frame["name"].values.tolist(),
            type_map=type_map,
            chunksize=250_000,
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="source_id",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        use_schema_file="/data3/epyc/data3/hipscat/tmp/allwise_schema.parquet",
        overwrite=True,
        resume=True,
        highest_healpix_order=8,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=20,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
    )
    runner.pipeline(args)
