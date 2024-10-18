import glob
import re

import dask
import dask.dataframe as dd
import hats_import.pipeline as runner
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import ascii as astropy_ascii
from dask.distributed import Client
from hats_import.hipscat_conversion.arguments import ConversionArguments
from hats_import.index.arguments import IndexArguments
from hats_import.catalog.arguments import ImportArguments
from tqdm import tqdm


def create_index(client):
    global_min = 4295806720
    global_max = 6917528997577384320
    num_row_groups = 3933

    increment = int((global_max - global_min) / num_row_groups)

    divisions = np.append(np.arange(start=global_min, stop=global_max, step=increment), global_max)
    divisions = divisions.tolist()

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/gaia",
        indexing_column="source_id",
        output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/",
        output_artifact_name="gaia_id_radec_index",
        include_healpix_29=True,
        include_order_pixel=False,
        extra_columns=["ra", "dec"],
        compute_partition_size=2_000_000_000,
        division_hints=divisions,
        drop_duplicates=True,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline_with_client(args, client)


def join_to_photometry():
    left_table = dd.read_parquet(
        path="/data3/epyc/data3/hats/catalogs/gaia_dr3/gaia_id_radec_index/dataset",
        engine="pyarrow",
    )

    files = glob.glob("/data3/epyc/data3/hipscat/raw/gaia/epoch_photometry/Epoch**")

    single_file = "/data3/epyc/data3/hipscat/raw/gaia/epoch_parquet/epoch_data_786097-786431.parquet"

    parquet_file = pq.ParquetFile(single_file)
    schema = parquet_file.schema.to_arrow_schema()
    schema = (
        schema.insert(0, pa.field("_healpix_29", pa.int64()))
        .append(pa.field("ra", pa.float32()))
        .append(pa.field("dec", pa.float32()))
    )

    for file in tqdm(files):
        match = re.match(r".*EpochPhotometry_([\d]+-[\d]+).csv.gz", str(file))
        file_id = match.group(1)

        astropy_table = astropy_ascii.read(file, format="ecsv")

        right_table = dd.from_pandas(astropy_table.to_pandas(), chunksize=2_000_000_000)
        result = left_table.merge(right_table, how="right", left_index=True, right_on="source_id")
        result = result.repartition(npartitions=1)

        result.to_parquet(
            f"/data3/epyc/data3/hats/raw/gaia/epoch_w_radec/bulk-{file_id}",
            engine="pyarrow",
            schema=schema,
            write_index=False,
        )


def import_catalog(client):
    files = glob.glob("/data3/epyc/data3/hats/raw/gaia/epoch_w_radec/epoch_*/*")

    args = ImportArguments(
        output_path="/data3/epyc/data3/hats/catalogs/gaia",
        output_artifact_name="epoch_photometry",
        input_file_list=files,
        file_reader="parquet",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hats/tmp/",
        highest_healpix_order=7,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline_with_client(args, client)


if __name__ == "__main__":
    dask.config.set({"dataframe.convert-string": False})
    client = Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=12,
        threads_per_worker=1,
    )
    # create_index(client)
    join_to_photometry()
    import_catalog(client)
