import hats_import.pipeline as runner
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments


def make_gaia_margins():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/gaia",
        output_path="/data3/epyc/data3/hats/catalogs/coarse_margins",
        output_artifact_name="gaia_10arcs",
        margin_threshold=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

def make_margins_distance():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hats/catalogs/gaia_dr3/gaia_edr3_distances",
        output_path="/data3/epyc/data3/hats/catalogs/coarse_margins",
        output_artifact_name="gaia_edr3_distances_10arcs",
        margin_threshold=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

def tic_margins():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hats/catalogs/tic/tic",
        output_path="/data3/epyc/data3/hats/catalogs/coarse_margins",
        output_artifact_name="tic_10arcs",
        margin_threshold=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


def ztf_dr14_margins():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hats/catalogs/ztf_dr14/ztf_object",
        output_path="/data3/epyc/data3/hats/catalogs/coarse_margins",
        output_artifact_name="ztf_object_10arcs",
        margin_threshold=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    # make_margins_distance()
    # tic_margins()
    ztf_dr14_margins()
