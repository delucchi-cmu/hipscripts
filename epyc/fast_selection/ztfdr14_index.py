import hipscat_import.pipeline as runner
from hipscat_import.index.arguments import IndexArguments

import ray
from ray.util.dask import enable_dask_on_ray, disable_dask_on_ray
from dask.distributed import Client

def create_index():

    context = ray.init(
        num_cpus=12,
        _temp_dir="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
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
        local_directory="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
        n_workers=12,
        threads_per_worker=1,
    ) as client:
        runner.pipeline_with_client(args, client)

    disable_dask_on_ray()

if __name__ == "__main__":
    create_index()