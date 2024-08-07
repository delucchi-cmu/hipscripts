"""Main method to enable command line execution"""

import hipscat_import.pipeline as runner
import pandas as pd
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader
import glob

if __name__ == "__main__":
    files = glob.glob("/epyc/data3/NEOWISE-yr8/irsa.ipac.caltech.edu/data/download/neowiser_year8/**.csv.bz2")
    files.sort()
    
    args = ImportArguments(
        output_artifact_name="neowise_yr8",
        input_file_list=files,
        file_reader=CsvReader(
            header=None,
            sep="|",
#             schema_file="/epyc/data3/hipscat/tmp/neowise_schema.parquet",
            chunksize=250_000,
        ),
#         resume=False,
        ra_column="RA",
        dec_column="DEC",
        sort_columns="SOURCE_ID",
        pixel_threshold=2_000_000,
        tmp_dir="/epyc/data3/hipscat/tmp/",
        highest_healpix_order=9,
        dask_n_workers=16,
        dask_threads_per_worker=1,
        dask_tmp="/epyc/data3/hipscat/tmp/",
        output_path="/epyc/data3/hipscat/test_catalogs/",
        completion_email_address="delucchi@andrew.cmu.edu",
        use_schema_file="/epyc/data3/hipscat/tmp/neowise_schema.parquet",
    )
    runner.pipeline(args)
