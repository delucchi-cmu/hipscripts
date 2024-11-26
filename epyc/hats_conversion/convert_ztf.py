import hats_import.pipeline as runner
from dask.distributed import Client
from hats_import.hipscat_conversion.arguments import ConversionArguments
from hats_import.index.arguments import IndexArguments


def convert_existing():

    with Client(
        local_directory="/data3/epyc/data3/hats/tmp/",
        n_workers=35,
        threads_per_worker=1,
    ) as client:
        # subcatalog = "ztf_dr14"
        # print("starting subcatalog", subcatalog)
        # args = ConversionArguments(
        #     input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/ztf_axs/{subcatalog}",
        #     output_path="/data3/epyc/data3/hats/catalogs/ztf_dr14",
        #     output_artifact_name=subcatalog,
        #     completion_email_address="delucchi@andrew.cmu.edu",
        # )
        # runner.pipeline_with_client(args, client)

        # subcatalog = "ztf_dr14_10arcs"
        # print("starting subcatalog", subcatalog)
        # args = ConversionArguments(
        #     input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/{subcatalog}",
        #     output_path="/data3/epyc/data3/hats/catalogs/ztf_dr14",
        #     output_artifact_name=subcatalog,
        #     completion_email_address="delucchi@andrew.cmu.edu",
        # )
        # runner.pipeline_with_client(args, client)

        # subcatalog = "ztf_source"
        # print("starting subcatalog", subcatalog)
        # args = ConversionArguments(
        #     input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_zource",
        #     output_path="/data3/epyc/data3/hats/catalogs/ztf_dr14",
        #     output_artifact_name=subcatalog,
        #     completion_email_address="delucchi@andrew.cmu.edu",
        # )
        # runner.pipeline_with_client(args, client)

        subcatalog = "zubercal"
        print("starting subcatalog", subcatalog)
        args = ConversionArguments(
            input_catalog_path=f"/data3/epyc/data3/hipscat/catalogs/{subcatalog}",
            output_path="/data3/epyc/data3/hats/catalogs/ztf_dr14",
            output_artifact_name=subcatalog,
            completion_email_address="delucchi@andrew.cmu.edu",
        )
        runner.pipeline_with_client(args, client)


if __name__ == "__main__":
    convert_existing()
