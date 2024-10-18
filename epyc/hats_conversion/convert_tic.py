import hats_import.pipeline as runner
from hats_import.hipscat_conversion.arguments import ConversionArguments


def convert_existing():
    args = ConversionArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/tic/tic",
        output_path="/data3/epyc/data3/hats/catalogs/tic",
        output_artifact_name="tic",
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


def convert_margin():
    args = ConversionArguments(
        input_catalog_path="/data3/epyc/data3/hipscat/catalogs/tic/tic_10arcs",
        output_path="/data3/epyc/data3/hats/catalogs/tic",
        output_artifact_name="tic_10arcs",
        dask_n_workers=10,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hats/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    convert_existing()
    convert_margin()
