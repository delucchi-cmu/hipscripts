import dask
import hipscat_import.pipeline as runner
import ray
from dask.distributed import Client
from hipscat_import.margin_cache.margin_cache_arguments import MarginCacheArguments
from ray.util.dask import disable_dask_on_ray, enable_dask_on_ray


def create_margin():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14",
        output_path="/data3/epyc/data3/hipscat/catalogs",
        output_artifact_name="ztf_dr14_10arcs",
        margin_threshold=10,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    create_margin()
