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


def transform_sources(data: pd.DataFrame) -> pd.DataFrame:
    """Explode repeating detections"""
    data = data[data.dup == 0].drop(["dup"], axis=1)

    ## band-specific columns to timedomain_columns
    g_column_map = {
        "catflags_g": "catflags",
        "fieldID_g": "fieldID",
        "mag_g": "mag",
        "magerr_g": "magerr",
        "mjd_g": "mjd",
        "rcID_g": "rcID",
    }
    g_columns = list(g_column_map.keys())
    r_column_map = {
        "catflags_r": "catflags",
        "fieldID_r": "fieldID",
        "mag_r": "mag",
        "magerr_r": "magerr",
        "mjd_r": "mjd",
        "rcID_r": "rcID",
    }
    r_columns = list(r_column_map.keys())
    i_column_map = {
        "catflags_i": "catflags",
        "fieldID_i": "fieldID",
        "mag_i": "mag",
        "magerr_i": "magerr",
        "mjd_i": "mjd",
        "rcID_i": "rcID",
    }
    i_columns = list(i_column_map.keys())

    explode_columns = list(g_column_map.values())

    just_i = data[REPEATED_COLUMNS + i_columns]
    just_i = just_i.rename(columns=i_column_map)
    just_i["band"] = "i"

    just_g = data[REPEATED_COLUMNS + g_columns]
    just_g = just_g.rename(columns=g_column_map)
    just_g["band"] = "g"

    just_r = data[REPEATED_COLUMNS + r_columns]
    just_r = just_r.rename(columns=r_column_map)
    just_r["band"] = "r"

    explodey = pd.concat([just_i, just_g, just_r]).explode(explode_columns)
    explodey = explodey[explodey["mag"].notna()]
    explodey = explodey.sort_values(["ps1_objid", "band", "mjd"])

    explodey = explodey.reset_index()

    return explodey


def per_file(file_index):
    input_pattern = f"/data/epyc/projects/lsd2/pzwarehouse/ztf_dr14/part-{str(file_index).zfill(5)}*"

    input_paths = glob.glob(input_pattern)
    if len(input_paths) != 1:
        print(f"BAD PATHS at {input_pattern}")
        return

    parquet_file = pq.read_table(input_paths[0])

    for index, smaller_table in enumerate(
        parquet_file.to_batches(max_chunksize=25_000)
    ):
        out_path = os.path.join(
            "/data3/epyc/data3/hipscat/raw/axs_ztf_shards_pivot/",
            f"part-{str(file_index).zfill(5)}-sub-{str(index).zfill(3)}.parquet",
        )
        if os.path.exists(out_path):
            continue

        data_frame = pa.Table.from_batches([smaller_table]).to_pandas()
        explodey = transform_sources(data_frame)
        explodey.to_parquet(out_path)
        del data_frame, explodey


def transform(client):
    futures = []
    for file_index in range(0, 500):
        futures.append(
            client.submit(
                per_file,
                file_index,
            )
        )
    complete = 0
    for _ in tqdm(as_completed(futures), desc="transforming", total=len(futures)):
        complete += 1
        if complete % 50 == 0:
            send_progress_email(complete)
        pass


def send_progress_email(num_complete):
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["Subject"] = f"epyc pivot execution PROGRESSING {num_complete}."
    msg["From"] = "updates@lsdb.io"
    msg["To"] = "delucchi@andrew.cmu.edu"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP("localhost")
    s.send_message(msg)
    s.quit()


def send_completion_email():
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["Subject"] = f"epyc pivot execution complete. eom."
    msg["From"] = "updates@lsdb.io"
    msg["To"] = "delucchi@andrew.cmu.edu"

    # Send the message via our own SMTP server.
    s = smtplib.SMTP("localhost")
    s.send_message(msg)
    s.quit()


if __name__ == "__main__":
    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=10,
        threads_per_worker=1,
    ) as client:
        transform(client)
        send_completion_email()
