"""Main method to enable command line execution

"""

import tempfile

import hipscat_import.run_import as runner
import pandas as pd
from hipscat_import.arguments import ImportArguments
from hipscat_import.file_readers import CsvReader

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        type_frame = pd.read_csv("tic_types.csv")
        type_map = dict(zip(type_frame["name"], type_frame["type"]))
        args = ImportArguments(
            catalog_name="neowise_10",
            input_file_list=["neowise.csv"],
            file_reader=CsvReader().read,
            ra_column="RA",
            dec_column="DEC",
            id_column="SOURCE_ID",
            pixel_threshold=1_000_000,
            tmp_dir=tmp_dir,
            overwrite=True,
            highest_healpix_order=10,
            dask_n_workers=1,
            dask_threads_per_worker=1,
            dask_tmp=tmp_dir,
            output_path="/home/delucchi/xmatch/catalogs/",
        )
        runner.run(args)
