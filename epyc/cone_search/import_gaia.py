import hipscat_import.pipeline as runner
import pandas as pd
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader


def do_import_50():
    file_names = [
        "GaiaSource_000000-003111.csv.gz",
        "GaiaSource_003112-005263.csv.gz",
        "GaiaSource_005264-006601.csv.gz",
        "GaiaSource_006602-007952.csv.gz",
        "GaiaSource_007953-010234.csv.gz",
        "GaiaSource_010235-012597.csv.gz",
        "GaiaSource_012598-014045.csv.gz",
        "GaiaSource_014046-015369.csv.gz",
        "GaiaSource_015370-016240.csv.gz",
        "GaiaSource_016241-017018.csv.gz",
        "GaiaSource_017019-017658.csv.gz",
        "GaiaSource_017659-018028.csv.gz",
        "GaiaSource_018029-018472.csv.gz",
        "GaiaSource_018473-019161.csv.gz",
        "GaiaSource_019162-019657.csv.gz",
        "GaiaSource_019658-020091.csv.gz",
        "GaiaSource_020092-020493.csv.gz",
        "GaiaSource_020494-020747.csv.gz",
        "GaiaSource_020748-020984.csv.gz",
        "GaiaSource_020985-021233.csv.gz",
        "GaiaSource_021234-021441.csv.gz",
        "GaiaSource_021442-021665.csv.gz",
        "GaiaSource_021666-021919.csv.gz",
        "GaiaSource_021920-022158.csv.gz",
        "GaiaSource_022159-022410.csv.gz",
        "GaiaSource_022411-022698.csv.gz",
        "GaiaSource_022699-022881.csv.gz",
        "GaiaSource_022882-023058.csv.gz",
        "GaiaSource_023059-023264.csv.gz",
        "GaiaSource_023265-023450.csv.gz",
        "GaiaSource_023451-023649.csv.gz",
        "GaiaSource_023650-023910.csv.gz",
        "GaiaSource_023911-024205.csv.gz",
        "GaiaSource_024206-024526.csv.gz",
        "GaiaSource_024527-025166.csv.gz",
        "GaiaSource_025167-025691.csv.gz",
        "GaiaSource_025692-026057.csv.gz",
        "GaiaSource_026058-026390.csv.gz",
        "GaiaSource_026391-026648.csv.gz",
        "GaiaSource_026649-027106.csv.gz",
        "GaiaSource_027107-027517.csv.gz",
        "GaiaSource_027518-027832.csv.gz",
        "GaiaSource_027833-028076.csv.gz",
        "GaiaSource_028077-028338.csv.gz",
        "GaiaSource_028339-028592.csv.gz",
        "GaiaSource_028593-028826.csv.gz",
        "GaiaSource_028827-029041.csv.gz",
        "GaiaSource_029042-029261.csv.gz",
        "GaiaSource_029262-029488.csv.gz",
        "GaiaSource_029489-029738.csv.gz",
    ]

    input_file_list = [f"/data3/epyc/data3/hipscat/raw/gaia/{file_name}" for file_name in file_names]

    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/test_catalogs",
        output_artifact_name="gaia_10csv",
        input_file_list=input_file_list[:10],
        input_format="csv.gz",
        file_reader=CsvReader(
            comment="#",
            schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        ),
        ra_column="ra",
        dec_column="dec",
        sort_columns="solution_id",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        use_schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        highest_healpix_order=8,
        dask_n_workers=10,
        resume=True,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


def do_import():
    args = ImportArguments(
        output_path="/data3/epyc/data3/hipscat/catalogs/gaia_dr3",
        output_artifact_name="gaia",
        input_path="/data3/epyc/data3/hipscat/raw/gaia/",
        input_format="csv.gz",
        file_reader=CsvReader(
            comment="#",
            schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        ),
        ra_column="ra",
        dec_column="dec",
        sort_columns="solution_id",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        use_schema_file="/astro/users/mmd11/git/scripts/epyc/cone_search/gaia_schema.parquet",
        highest_healpix_order=8,
        dask_n_workers=40,
        dask_threads_per_worker=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        completion_email_address="delucchi@andrew.cmu.edu",
    )
    runner.pipeline(args)


if __name__ == "__main__":
    do_import()
