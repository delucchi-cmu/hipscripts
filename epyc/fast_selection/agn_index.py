import hipscat_import.pipeline as runner
from hipscat_import.index.arguments import IndexArguments

def create_index():
    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/dr16q_constant",
        indexing_column="SDSS_NAME",
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="agn_sdss_id",
        # include_hipscat_index=False,
        compute_partition_size=20_000_000,
        dask_n_workers=5,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    create_index()