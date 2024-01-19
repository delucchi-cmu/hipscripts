import hipscat_import.pipeline as runner
from hipscat_import.index.arguments import IndexArguments

def create_index():

    import numpy as np

    divisions = [f"Gaia DR3 {i}" for i in range(10000, 99999, 12)]
    divisions.append("Gaia DR3 999999988604363776")

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic",
        indexing_column="designation",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="gaia_designation",
        include_hipscat_index=False,
        compute_partition_size=20_000_000_000,
        divisions=divisions,
        drop_duplicates=False,
        dask_n_workers=10,
        overwrite=True,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    create_index()