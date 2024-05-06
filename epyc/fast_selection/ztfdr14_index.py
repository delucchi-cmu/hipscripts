import dask
import hipscat_import.pipeline as runner
import ray
from dask.distributed import Client
from hipscat_import.index.arguments import IndexArguments
from ray.util.dask import disable_dask_on_ray, enable_dask_on_ray


def create_index_ray():

    context = ray.init(
        num_cpus=12,
        _temp_dir="/data3/epyc/projects3/mmd11/ray",
    )
    dask.config.set(
        {
            "dataframe.shuffle.method": "disk",
            "dataframe.shuffle.algorithm": "disk",
            "shuffle": "disk",
        }
    )

    enable_dask_on_ray()
    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14",
        indexing_column="ps1_objid",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="ztf_id",
        include_hipscat_index=False,
        compute_partition_size=20_000_000,
        completion_email_address="delucchi@andrew.cmu.edu",
        overwrite=True,
    )

    with Client(
        local_directory="/data3/epyc/projects3/mmd11/ray",
        n_workers=12,
        threads_per_worker=1,
        set_as_default=True,
    ) as client:
        runner.pipeline_with_client(args, client)

    disable_dask_on_ray()


def create_index():

    import numpy as np

    divisions = np.arange(start=71150119096299949, stop=215050952450164082, step=30604175532510)
    divisions = np.append(divisions, 215050952450164082)

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14",
        indexing_column="ps1_objid",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="ztf_id_div_dup",
        include_hipscat_index=False,
        compute_partition_size=20_000_000,
        divisions=divisions,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    create_index()
