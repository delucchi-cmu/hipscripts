import glob
import os

import hipscat_import.cross_match.run_macauff_import as runner
from dask.distributed import Client
from hipscat.catalog.association_catalog.association_catalog import AssociationCatalog
from hipscat.io import file_io
from hipscat_import.catalog.file_readers import CsvReader
from hipscat_import.cross_match.macauff_arguments import MacauffArguments
from hipscat_import.cross_match.macauff_metadata import from_yaml


def gaia_to_catwise_schema():
    yaml_input_file = "macauff_metadata.yml"
    from_yaml(yaml_input_file, ".")
    matches_schema_file = "macauff_GaiaDR3xCatWISE2020_matches.parquet"
    single_metadata = file_io.read_parquet_metadata(matches_schema_file)
    schema = single_metadata.schema.to_arrow_schema()


def gaia_to_catwise_2309():
    macauff_data_dir = "/data3/epyc/data3/hipscat/raw/macauff_results/rds/project/iris_vol3/rds-iris-ip005/tjw/dr3_catwise_allskytest/output_csvs/"
    matches_schema_file = "macauff_GaiaDR3xCatWISE2020_matches.parquet"

    files = glob.glob(f"{macauff_data_dir}/**/*.csv")
    files.sort()

    args = MacauffArguments(
        output_path="/data3/epyc/data3/hipscat/test_catalogs/",
        output_artifact_name="macauff_association",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/macauff/",
        left_catalog_dir="/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic",
        left_ra_column="gaia_ra",
        left_dec_column="gaia_dec",
        left_id_column="gaia_source_id",
        right_catalog_dir="/epyc/projects3/sam_hipscat/catwise2020/catwise2020/",
        right_ra_column="catwise_ra",
        right_dec_column="catwise_dec",
        right_id_column="catwise_name",
        input_file_list=files,
        # input_path=macauff_data_dir,
        input_format="csv",
        overwrite=True,
        file_reader=CsvReader(schema_file=matches_schema_file, header=None),
        metadata_file_path=matches_schema_file,
        # progress_bar=False,
    )

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/macauff/",
        n_workers=5,
        threads_per_worker=1,
    ) as client:
        runner.run(args, client)

    ## Check that the association data can be parsed as a valid association catalog.
    catalog = AssociationCatalog.read_from_hipscat(args.catalog_path)


if __name__ == "__main__":
    gaia_to_catwise_2309()
