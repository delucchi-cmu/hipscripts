"""Main method to enable command line execution
"""

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader
import glob

if __name__ == "__main__":
    files = glob.glob("/epyc/data3/hipscat/raw/allwise_raw/wise-allwise-cat-part**")
    files.sort()
    
    args = ImportArguments(
        output_artifact_name="allwise",
        input_file_list=files,
        file_reader=CsvReader(
            header=None,
            sep="|",
            chunksize=250_000,
#             schema_file="/epyc/data3/hipscat/raw/allwise_schema.parquet",
        ),
        ra_column="ra",
        dec_column="dec",
        sort_columns="source_id",
        pixel_threshold=250_000,
        tmp_dir="/epyc/data3/hipscat/tmp/",
        highest_healpix_order=10,
        use_schema_file="/epyc/data3/hipscat/raw/allwise_schema.parquet",
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=12,
        dask_tmp="/epyc/data3/hipscat/tmp/",
        output_path="/epyc/data3/hipscat/test_catalogs/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)
#     print(args.provenance_info())
