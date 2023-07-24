import hipscat_import.pipeline as runner
from hipscat_import.soap.arguments import SoapArguments


def create_association():
    args = SoapArguments(
        object_catalog_dir="/data3/epyc/data3/hipscat/catalogs/ps1/ps1_otmo",
        object_id_column="objID",
        source_catalog_dir="/data3/epyc/data3/hipscat/catalogs/zubercal/",
        source_object_id_column="objectid",
        output_path="/data3/epyc/data3/hipscat/catalogs/ps1/",
        output_catalog_name="ps1_to_zubercal",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        # overwrite=True,
        dask_n_workers=20,
        dask_threads_per_worker=1,
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)

if __name__ == "__main__":
    create_association()