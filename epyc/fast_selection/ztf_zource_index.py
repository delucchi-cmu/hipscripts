import hipscat_import.pipeline as runner
from hipscat_import.index.arguments import IndexArguments
import numpy as np

def create_index():
    divisions = np.arange(start = 71150119096299949, stop = 215050952450164082, step = 30604175532510)
    divisions = np.append(divisions, 215050952450164082)
    divisions = divisions.tolist()

    args = IndexArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_zource",
        indexing_column="ps1_objid",
        drop_duplicates=True,
        include_hipscat_index=False,
        division_hints=divisions,
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="ztf_zource_id_div_dup",
        compute_partition_size=2_000_000_000,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    print("args generated")
    runner.pipeline(args)

if __name__ == "__main__":
    create_index()