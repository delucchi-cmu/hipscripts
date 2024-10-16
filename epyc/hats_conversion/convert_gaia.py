import hats_import.pipeline as runner
import numpy as np
from dask.distributed import Client
from hats_import.hipscat_conversion.arguments import ConversionArguments
from hats_import.index.arguments import IndexArguments


def convert_existing():

    with Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=20,
        threads_per_worker=1,
    ) as client:
        subcatalog = "gaia"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/gaia_dr3/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)

        subcatalog = "gaia_10arcs"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/gaia_dr3/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)

        subcatalog = "gaia_edr3_distances"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/gaia_dr3/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)

        subcatalog = "gaia_edr3_distances_10arcs"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/gaia_dr3/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)


def create_index():
    print("starting source_id index")

    with Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=20,
        threads_per_worker=1,
    ) as client:

        global_min = 4295806720
        global_max = 6917528997577384320
        num_row_groups = 3933

        increment = int((global_max - global_min) / num_row_groups)

        divisions = np.append(np.arange(start=global_min, stop=global_max, step=increment), global_max)
        divisions = divisions.tolist()

        args = IndexArguments(
            input_catalog_path="/data3/epyc/data3/hats/catalogs/gaia_dr3",
            indexing_column="source_id",
            output_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/",
            output_artifact_name="gaia_source_id_index",
            include_healpix_29=False,
            compute_partition_size=2_000_000_000,
            division_hints=divisions,
            drop_duplicates=False,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)


if __name__ == "__main__":
    convert_existing()
    # create_index()
