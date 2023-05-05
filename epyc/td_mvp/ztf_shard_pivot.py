import pandas as pd

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import glob
import pyarrow.parquet as pq
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader
from dask.distributed import Client
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

def per_file(file_name):
    name_only = os.path.basename(file_name)
    data_frame = pd.read_parquet(file_name, engine="pyarrow")
    explodey = transform_sources(data_frame)

    new_name = os.path.join("/data3/epyc/data3/hipscat/raw/ztf_shards_pivot/", name_only)
    explodey.to_parquet(new_name)


def transform(client):
    # file_names = ["/data3/epyc/data3/hipscat/raw/ztf_shards/part-00128-shard-6.parquet",
    #                "/data3/epyc/data3/hipscat/raw/ztf_shards/part-00128-shard-7.parquet"]
    file_names = glob.glob("/data3/epyc/data3/hipscat/raw/ztf_shards/**parquet")
    futures = []
    for file_path in file_names:
        futures.append(
            client.submit(
            per_file, 
            file_name = file_path,
            )
        )
    for _ in tqdm(
        as_completed(futures),
        desc="transforming",
        total=len(futures)
    ):
        pass

if __name__ == "__main__":

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=42,
        threads_per_worker=1,
    ) as client:
        transform(client)
