"""
Smaller catalogs.

DES dr2
alerce
erosita
"""

import hats_import.pipeline as runner
from dask.distributed import Client
from hats_import.hipscat_conversion.arguments import ConversionArguments


def convert_existing():

    with Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=30,
        threads_per_worker=1,
    ) as client:
        subcatalog = "des_dr2"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/des/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/des",
            tmp_dir="/data3/epyc/data3/hats/tmp/",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)

        subcatalog = "alerce_nested"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/test_catalogs/alerce/alerce_nested",
            output_path="/data3/epyc/data3/hats/catalogs/alerce",
            tmp_dir="/data3/epyc/data3/hats/tmp/",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)

        # /astro/store/epyc3/projects3/max_hipscat/erosita/

        subcatalog = "erosita_dr1_erass1"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path="/astro/store/epyc3/projects3/max_hipscat/erosita/erosita_dr1_erass1",
            output_path="/data3/epyc/data3/hats/catalogs/erosita",
            tmp_dir="/data3/epyc/data3/hats/tmp/",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)


if __name__ == "__main__":
    convert_existing()
