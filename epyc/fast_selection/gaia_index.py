import hipscat_import.pipeline as runner
import numpy as np
from hipscat_import.index.arguments import IndexArguments


def create_index():

    # import numpy as np

    # divisions = [f"Gaia DR3 {i}" for i in range(10000, 99999, 12)]
    # divisions.append("Gaia DR3 999999988604363776")

    global_min = 4295806720
    global_max = 6917528997577384320
    num_row_groups = 3933

    increment = int((global_max - global_min) / num_row_groups)

    divisions = np.append(np.arange(start=global_min, stop=global_max, step=increment), global_max)
    divisions = divisions.tolist()

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3/gaia",
        indexing_column="source_id",
        output_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3/",
        output_artifact_name="gaia_source_id_index",
        include_hipscat_index=False,
        compute_partition_size=2_000_000_000,
        division_hints=divisions,
        drop_duplicates=False,
        dask_n_workers=20,
        overwrite=True,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


def create_index_10():

    # import numpy as np

    # divisions = [f"Gaia DR3 {i}" for i in range(10000, 99999, 12)]
    # divisions.append("Gaia DR3 999999988604363776")

    global_min = 4295806720
    global_max = 6917528997577384320
    num_row_groups = 10

    increment = int((global_max - global_min) / num_row_groups)

    divisions = np.append(np.arange(start=global_min, stop=global_max, step=increment), global_max)
    divisions = divisions.tolist()

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/test_catalogs/gaia_10csv",
        indexing_column="source_id",
        output_path="/data3/epyc/data3/hipscat/test_catalogs/",
        output_artifact_name="gaia_source_id_index",
        include_hipscat_index=False,
        compute_partition_size=2_000_000_000,
        division_hints=divisions,
        drop_duplicates=False,
        dask_n_workers=20,
        overwrite=True,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    create_index()
