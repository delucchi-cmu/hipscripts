import hipscat_import.pipeline as runner
from hipscat_import.soap.arguments import SoapArguments


def create_association():
    args = SoapArguments(
        object_catalog_dir="/data3/epyc/data3/hipscat/catalogs/ps1/ps1_otmo",
        object_id_column="objID",
        source_catalog_dir="/data3/epyc/data3/hipscat/catalogs/ps1/ps1_detection",
        source_object_id_column="objID",
        output_path="/data3/epyc/data3/hipscat/test_catalogs/soap/",
        output_catalog_name="ps1_otmo_to_detection",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/ps1/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/ps1/",
        overwrite=True,
        dask_n_workers=10,
        dask_threads_per_worker=1,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    create_association()
