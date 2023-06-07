import hipscat_import.association.run_association as a_runner
from hipscat_import.association.arguments import AssociationArguments

def create_association():
    args = AssociationArguments(
        primary_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ps1/ps1_otmo",
        primary_id_column="objID",
        primary_join_column="objID",
        join_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ps1/ps1_detection",
        join_id_column="objID",
        join_foreign_key="objID",
        output_path="/data3/epyc/data3/hipscat/catalogs/ps1/",
        output_catalog_name="ps1_otmo_to_detection",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/ps1/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/ps1/",
        overwrite=True,
    )
    a_runner.run_with_client(args)