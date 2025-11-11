import numpy as np
from dask.distributed import Client
from hats_import import CollectionArguments, pipeline_with_client
from pathlib import Path


def do_work():

    with Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=20,
        threads_per_worker=1,
    ) as client:
        ## input paths:
        raw_dir = Path("/data3/epyc/data3/hipscat/raw/gaia/")
        file_list = list(raw_dir.glob("GaiaSource*"))
        
        
        ## Index division hints
        global_min = 4295806720
        global_max = 6917528997577384320
        num_row_groups = 3933

        increment = int((global_max - global_min) / num_row_groups)

        divisions = np.append(np.arange(start=global_min, stop=global_max, step=increment), global_max)
        divisions = divisions.tolist()

        args = (
            CollectionArguments(
                completion_email_address="delucchi@andrew.cmu.edu",
                output_artifact_name="gaia_dr3",
                output_path="/data3/epyc/data3/hats/catalogs/v06",
                progress_bar=True,
            )
            .catalog(
                output_artifact_name="gaia",
                input_file_list=file_list[:2],
                file_reader="csv",
                ra_column="ra",
                dec_column="dec",
                sort_columns="source_id",
                highest_healpix_order=10,
                pixel_threshold=1_000_000,
            )
            .add_margin(margin_threshold=10.0, is_default=True)
            .add_index(
                indexing_column="source_id",
                output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/",
                output_artifact_name="gaia_source_id_index",
                include_healpix_29=False,
                include_order_pixel=True,
                compute_partition_size=2_000_000_000,
                division_hints=divisions,
                drop_duplicates=False,
            )
        )


#         pipeline_with_client(args)


if __name__ == "__main__":
    do_work()
