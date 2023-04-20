import glob
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def singleindex():
    """pylint"""
    # data_frame = pd.DataFrame()
    data = {
        "obs_id":["star1_1", "star1_2", "star1_3", "star1_4", "galaxy1_1", "galaxy1_2", "galaxy2_1", "galaxy2_2"],
        "obj_id":["star1", "star1", "star1", "star1", "galaxy1", "galaxy1", "galaxy2", "galaxy2"],
        "band":["r", "r", "i", "i", "r", "r", "r", "r"]
    }

    ras = np.random.rand(8)
    ras = [300 + 3 * x for x in ras]
    dec = np.random.rand(8)
    dec = [-40 + 2 * x for x in dec]
    mags=np.random.rand(8)
    mags = [150 + 6 * x for x in mags]
    df = pd.DataFrame(data)
    df["ra"] = ras
    df["dec"] = dec
    df["mag"] = mags    
    df = df.set_index("obs_id")

    print(df)
    df = df.sort_index()
    df.to_parquet("/home/delucchi/git/parquet/tests/hipscat_import/data/test_formats/pandasindex.parquet")


def multiindex():
    """foo"""
    arrays = [
        ["star1", "star1", "star1", "star1", "galaxy1", "galaxy1", "galaxy2", "galaxy2"],
        ["r", "r", "i", "i", "r", "r", "r", "r"],
    ]
    df = pd.DataFrame(list(zip(arrays[0], arrays[1])),
        columns=["obj_id", "band"],
    )

    ras = np.random.rand(8)
    ras = [300 + x for x in ras]
    dec = np.random.rand(8)
    dec = [-40 + x for x in dec]
    mags=np.random.rand(8)
    mags = [150 + 6 * x for x in mags]
    df = pd.DataFrame(index=pd.MultiIndex.from_frame(df))
    df["ra"] = ras
    df["dec"] = dec
    df["mag"] = mags    

    print(df)
    print(df.index)
    print(df.index.name)
    print(df.index.names)
    df = df.sort_index()
    df.to_parquet("/home/delucchi/git/parquet/tests/hipscat_import/data/test_formats/multiindex.parquet")

if __name__ == "__main__":
    s = time.time()

    multiindex()
    singleindex()

    e = time.time()
    print(f"Elapsed Time: {e-s}")
