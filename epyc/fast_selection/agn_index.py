import hipscat_import.pipeline as runner
from hipscat_import.index.arguments import IndexArguments

import ray
from ray.util.dask import enable_dask_on_ray, disable_dask_on_ray
from dask.distributed import Client

from dask.diagnostics import ProgressBar


def create_index_ray():
    context = ray.init(
        num_cpus=12,
        _temp_dir="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
    )

    enable_dask_on_ray()
    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/dr16q_constant",
        indexing_column="SDSS_NAME",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="agn_sdss_id2",
        include_hipscat_index=False,
        compute_partition_size=20_000_000,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    with Client(
        local_directory="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
        n_workers=12,
        threads_per_worker=1,
    ) as client:
        runner.pipeline_with_client(args, client)

    disable_dask_on_ray()

def create_index():
    ProgressBar().register()
    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/dr16q_constant",
        indexing_column="SDSS_NAME",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="agn_sdss_id3",
        include_hipscat_index=False,
        compute_partition_size=1_000_000_000,
        dask_n_workers=10,
        overwrite=True,
        dask_threads_per_worker=1,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    create_index()