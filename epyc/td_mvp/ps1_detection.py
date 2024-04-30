import glob

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.distributed import Client
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader


def import_objects(client):
    use_columns = [
        "objID",
        "uniquePspsP2id",
        "detectID",
        "filterID",
        "obsTime",
        "ra",
        "dec",
        "raErr",
        "decErr",
        "expTime",
        "psfFlux",
        "psfFluxErr",
        "apFlux",
        "apFluxErr",
        "kronFlux",
        "kronFluxErr",
        "zp",
        "infoFlag",
        "infoFlag2",
        "infoFlag3",
    ]
    type_frame = pd.read_csv("ps1_detections_types.csv")
    type_map = dict(zip(type_frame["name"], type_frame["type"]))
    type_names = type_frame["name"].values.tolist()

    in_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/pan_starrs/detection/detection**.csv")
    in_file_paths.sort()
    print(len(in_file_paths))
    args = ImportArguments(
        output_catalog_name="ps1_detection",
        input_file_list=in_file_paths,
        input_format="csv",
        file_reader=CsvReader(
            header=None,
            index_col=False,
            column_names=type_names,
            # names=type_names,
            type_map=type_map,
            chunksize=250_000,
            usecols=use_columns,
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="objID",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        # overwrite=True,
        resume=True,
        highest_healpix_order=10,
        # debug_stats_only=True,
        dask_n_workers=20,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ps1/",
    )
    runner.run_with_client(args, client=client)


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
        compute_partition_size=1024 * 1024 * 1024,
        overwrite=True,
    )
    a_runner.run(args)


def send_completion_email():
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["Subject"] = f"epyc execution complete. eom."
    msg["From"] = "updates@lsdb.io"
    msg["To"] = "delucchi@andrew.cmu.edu"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP("localhost")
    s.send_message(msg)
    s.quit()


if __name__ == "__main__":
    import ray
    from ray.util.dask import disable_dask_on_ray, enable_dask_on_ray

    context = ray.init(
        num_cpus=12,
        _temp_dir="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
    )

    enable_dask_on_ray()

    # do any dask stuff and it will run on ray

    with Client(
        local_directory="/data3/epyc/projects3/mmd11_hipscat/ray_spill",
        n_workers=12,
        threads_per_worker=1,
    ) as client:
        # import_objects(client)
        # import_objects(client)
        create_association()
        send_completion_email()
