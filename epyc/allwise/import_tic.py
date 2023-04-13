import tempfile

import pandas as pd

import hipscat_import.run_import as runner
from hipscat_import.arguments import ImportArguments
from hipscat_import.file_readers import CsvReader

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        type_frame = pd.read_csv("tic_types.csv")
        type_map = dict(zip(type_frame["name"], type_frame["type"]))
        args = ImportArguments(
            catalog_name="tic_1",
            input_file_list=["/data3/epyc/data3/hipscat/tic_csv/tic_dec90_00S__88_00S.csv.gz"],
            file_reader=CsvReader(
                header=None,
                column_names=type_frame["name"].values.tolist(),
                type_map=type_map,
                chunksize=250_000,
            ).read,
            ra_column="ra",
            dec_column="dec",
            id_column="ID",
            pixel_threshold=1_000_000,
            tmp_dir=tmp_dir,
            overwrite=True,
            highest_healpix_order=10,
            dask_n_workers=1,
            dask_threads_per_worker=1,
            dask_tmp=tmp_dir,
            output_path="/data3/epyc/data3/hipscat/catalogs/",
        )
        runner.run(args)
