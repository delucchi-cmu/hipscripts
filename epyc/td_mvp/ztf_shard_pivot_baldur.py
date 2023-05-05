import pandas as pd

import glob
import os
from dask.distributed import Client, as_completed
from tqdm import tqdm

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
    # print("explodey size (nan-filtered)", len(explodey))

    explodey = explodey.reset_index()

    return explodey

def per_file(in_path, out_path):
    data_frame = pd.read_parquet(in_path, engine="pyarrow")
    explodey = transform_sources(data_frame)

    explodey.to_parquet(out_path)
    del data_frame, explodey


def transform(client):
    in_file_paths = glob.glob("/epyc/data3/hipscat/raw/ztf_shards/**parquet")
    in_file_names = [os.path.basename(file_name) for file_name in in_file_paths]
    in_file_names = set(in_file_names)
    out_file_paths = glob.glob("/epyc/data3/hipscat/raw/ztf_shards_pivot/**parquet")
    out_file_names = [os.path.basename(file_name) for file_name in out_file_paths]
    out_file_names = set(out_file_names)

    target_file_names = in_file_names.difference(out_file_names)
    target_file_names = [file_name  for file_name in target_file_names if file_name.__hash__() %2==0]
    print(len(target_file_names))

    futures = []
    for file_name in target_file_names:
        futures.append(
            client.submit(
            per_file, 
            in_path = os.path.join("/epyc/data3/hipscat/raw/ztf_shards/", file_name),
            out_path = os.path.join("/epyc/data3/hipscat/raw/ztf_shards_pivot/", file_name)
            )
        )
    for _ in tqdm(
        as_completed(futures),
        desc="transforming",
        total=len(futures)
    ):
        pass


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
        local_directory="/epyc/data3/hipscat/tmp/baldur",
        n_workers=42,
        threads_per_worker=1,
    ) as client:
        transform(client)
        send_completion_email()
