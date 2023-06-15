"""Main method to enable command line execution

ztf schema:
required group field_id=-1 schema {
  optional int64 field_id=-1 ps1_objid;
  optional double field_id=-1 ra;
  optional double field_id=-1 dec;
  optional binary field_id=-1 filterName (String);
  optional float field_id=-1 midPointTai;
  optional float field_id=-1 psFlux;
  optional float field_id=-1 psFluxErr;
  optional int64 field_id=-1 __index_level_0__;
}

>> python3 ztf_epyc.py &> log.txt
"""

import pandas as pd

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader
from dask.distributed import Client


def filter_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where dup is true"""
    return data[data.dup == 0].drop(["dup"], axis=1)


#### -----------------
## Columns that will be repeated per object
REPEATED_COLUMNS = [
    "ps1_objid",
    "ra",
    "dec",
    "ps1_gMeanPSFMag",
    "ps1_rMeanPSFMag",
    "ps1_iMeanPSFMag",
    "nobs_g",
    "nobs_r",
    "nobs_i",
    "mean_mag_g",
    "mean_mag_r",
    "mean_mag_i",
]


def import_objects(client):
    args = ImportArguments(
        catalog_name="ztf_dr14",
        # input_file_list=["/data3/epyc/data3/hipscat/raw/ztf_shards/part-00193-shard-7.parquet"],
        input_path="/data3/epyc/data3/hipscat/raw/ztf_shards/",
        input_format="parquet",
        file_reader=ParquetReader(
            # chunksize=50_000,
            columns=REPEATED_COLUMNS
            + ["dup"],
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        filter_function=filter_duplicates,
        overwrite=True,
        highest_healpix_order=8,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr18/",
    )
    runner.run_with_client(args, client=client)


def import_sources(client):
    args = ImportArguments(
        output_catalog_name="ztf_source",
        # input_file_list=["/data3/epyc/data3/hipscat/raw/ztf_shards/part-00193-shard-7.parquet"],
        input_path="/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/",
        input_format="parquet",
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=2_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=11,
        resume=True,
        dask_n_workers=48,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_may18/",
    )
    runner.run_with_client(args, client=client)


import hipscat_import.association.run_association as a_runner
from hipscat_import.association.arguments import AssociationArguments


def create_association():
    args = AssociationArguments(
        primary_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr18/ztf_dr14",
        primary_id_column="ps1_objid",
        primary_join_column="ps1_objid",
        join_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_may18/ztf_source",
        join_id_column="ps1_objid",
        join_foreign_key="ps1_objid",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_may18/",
        output_catalog_name="ztf_object_to_source",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
    )
    a_runner.run_with_client(args)


def send_completion_email():
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["Subject"] = f"epyc execution complete. eom."
    msg["From"] = "delucchi@gmail.com"
    msg["To"] = "delucchi@andrew.cmu.edu"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP("localhost")
    s.send_message(msg)
    s.quit()


if __name__ == "__main__":
    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=42,
        threads_per_worker=1,
    ) as client:
        # import_objects(client)
        import_sources(client)
        # create_association()
        send_completion_email()
