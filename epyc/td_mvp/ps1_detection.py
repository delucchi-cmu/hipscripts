
import pandas as pd

import hipscat_import.catalog.run_import as runner
import pandas as pd
import glob
import pyarrow as pa
import pyarrow.parquet as pq
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import CsvReader
from dask.distributed import Client

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
            usecols=use_columns
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
        output_path="/data3/epyc/data3/hipscat/catalogs/td_mvp_test/",
    )
    runner.run_with_client(args, client=client)

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
        # import_objects(client)
        import_objects(client)
        # create_association()
        send_completion_email()