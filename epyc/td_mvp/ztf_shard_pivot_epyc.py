import pandas as pd

import glob
import os
from dask.distributed import Client, as_completed
from tqdm import tqdm
import pyarrow as pa 
import pyarrow.parquet as pq

#### -----------------
## Columns that will be repeated per object
REPEATED_COLUMNS = [
    "ps1_objid", "ra", "dec", "ps1_gMeanPSFMag", "ps1_rMeanPSFMag", "ps1_iMeanPSFMag",
    "nobs_g", "nobs_r", "nobs_i","mean_mag_g","mean_mag_r","mean_mag_i"]

def transform_sources(data: pd.DataFrame) -> pd.DataFrame:
    """Explode repeating detections"""
    data = data[data.dup == 0].drop(["dup"], axis=1)

    ## band-specific columns to timedomain_columns
    g_column_map = {"catflags_g":"catflags",
                            "fieldID_g" : "fieldID", 
                            "mag_g":"mag",
                            "magerr_g":"magerr",
                            "mjd_g":"mjd", 
                            "rcID_g":"rcID"}
    g_columns = list(g_column_map.keys())
    r_column_map = {"catflags_r":"catflags",
                            "fieldID_r" : "fieldID", 
                            "mag_r":"mag",
                            "magerr_r":"magerr",
                            "mjd_r":"mjd", 
                            "rcID_r":"rcID"}
    r_columns = list(r_column_map.keys())
    i_column_map = {"catflags_i":"catflags",
                            "fieldID_i" : "fieldID", 
                            "mag_i":"mag",
                            "magerr_i":"magerr",
                            "mjd_i":"mjd", 
                            "rcID_i":"rcID"}
    i_columns = list(i_column_map.keys())

    explode_columns = list(g_column_map.values())

    just_i = data[REPEATED_COLUMNS+i_columns]
    just_i = just_i.rename(columns=i_column_map)
    just_i['band'] = 'i'

    just_g = data[REPEATED_COLUMNS+g_columns]
    just_g = just_g.rename(columns=g_column_map)
    just_g['band'] = 'g'

    just_r = data[REPEATED_COLUMNS+r_columns]
    just_r = just_r.rename(columns=r_column_map)
    just_r['band'] = 'r'

    explodey = pd.concat([just_i, just_g, just_r]).explode(explode_columns)
    explodey = explodey[explodey['mag'].notna()]
    explodey = explodey.sort_values(["ps1_objid", 'band', "mjd"])

    explodey = explodey.reset_index()

    return explodey

def per_file(file_name):
    in_path = os.path.join("/data3/epyc/data3/hipscat/raw/ztf_shards/", file_name)

    file_minus = file_name[0:-8]
    parquet_file = pq.read_table(in_path)
    index = 1
    for smaller_table in parquet_file.to_batches(max_chunksize=50_000):
        out_path = os.path.join("/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/", f"{file_minus}-sub-{index}.parquet")
        index += 1
        if  os.path.exists(out_path):
            continue

        sub_table = pa.Table.from_batches([smaller_table])
        small = 1
        for even_smaller in sub_table.to_batches(max_chunksize=10_000):
            out_path = os.path.join("/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/", f"{file_minus}-sub-{index}-small-{small}.parquet")
            small += 1
            if os.path.exists(out_path):
                continue
            data_frame = pa.Table.from_batches([even_smaller]).to_pandas()
            explodey = transform_sources(data_frame)        
            explodey.to_parquet(out_path)
            del data_frame, explodey


def transform(client):
    in_file_paths = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards/**parquet")
    # in_file_paths = ["/data3/epyc/data3/hipscat/raw/ztf_shards/part-00028-shard-16.parquet"]
    in_file_names = [os.path.basename(file_name) for file_name in in_file_paths]
    # in_file_names = set(in_file_names)
    in_file_names.sort()

    futures = []
    for file_name in in_file_names:
        futures.append(
            client.submit(
            per_file,
            file_name,
            )
        )
    complete = 0
    for _ in tqdm(
        as_completed(futures),
        desc="transforming",
        total=len(futures)
    ):
        complete +=1
        if complete %1_000 == 0:
            send_progress_email(complete)
        pass


def send_progress_email(num_complete):
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg['Subject'] = f'epyc pivot execution PROGRESSING {num_complete}.'
    msg['From'] = 'updates@lsdb.io'
    msg['To'] = 'delucchi@andrew.cmu.edu'

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

def send_completion_email():
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg['Subject'] = f'epyc pivot execution complete. eom.'
    msg['From'] = 'updates@lsdb.io'
    msg['To'] = 'delucchi@andrew.cmu.edu'

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

if __name__ == "__main__":

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=42,
        threads_per_worker=1,
    ) as client:
        transform(client)
        send_completion_email()
