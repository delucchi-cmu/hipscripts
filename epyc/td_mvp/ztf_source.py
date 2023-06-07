"""Main method to enable command line execution

"""

import pandas as pd

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader
from dask.distributed import Client


def import_sources(client):
    args = ImportArguments(
        output_catalog_name="ztf_source",
        # input_file_list=["/data3/epyc/data3/hipscat/raw/axs_ztf_shards_pivot/part-00000-sub-109.parquet"],
        input_path="/data3/epyc/data3/hipscat/raw/axs_ztf_shards_pivot/",
        input_format="parquet",
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/ztf_jun01/",
        overwrite=True,
        highest_healpix_order=10,
        resume=True,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/ztf_jun01/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_jun01/",
    )
    runner.run_with_client(args, client=client)

import hipscat_import.association.run_association as a_runner
from hipscat_import.association.arguments import AssociationArguments

def create_association():
    args = AssociationArguments(
        primary_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr18/ztf_dr14",
        primary_id_column="ps1_objid",
        primary_join_column="ps1_objid",
        join_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_jun01/ztf_source",
        join_id_column="ps1_objid",
        join_foreign_key="ps1_objid",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_axs/",
        output_catalog_name="ztf_object_to_source",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/ztf_jun01/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/ztf_jun01/",
        overwrite=True,
    )
    a_runner.run_with_client(args)

def send_completion_email():
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg['Subject'] = f'epyc execution complete. eom.'
    msg['From'] = 'delucchi@gmail.com'
    msg['To'] = 'delucchi@andrew.cmu.edu'

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

if __name__ == "__main__":

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=10,
        threads_per_worker=1,
    ) as client:
        import_sources(client)
        # create_association()
        send_completion_email()
