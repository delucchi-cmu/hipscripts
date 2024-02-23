import hipscat_import.pipeline as runner
from hipscat_import.margin_cache.margin_cache_arguments import MarginCacheArguments

def make_margins():
    args = MarginCacheArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3/gaia",
        output_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3",
        output_artifact_name="gaia_10arcs",
        margin_threshold=10,
        dask_n_workers=30,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    make_margins()