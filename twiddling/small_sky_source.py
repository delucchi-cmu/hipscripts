"""Create the small_sky_source catalog!"""

import time

import numpy as np
import pandas as pd

if __name__ == "__main__":
    s = time.time()

    object_frame = pd.read_csv(
        "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/small_sky/catalog.csv"
    )

    source_frame = object_frame.copy()

    source_frame["object_ra"] = object_frame["ra"]
    source_frame["object_dec"] = object_frame["dec"]

    source_frame["object_id"] = object_frame["id"]
    source_frame["object_id"] = source_frame["object_id"].astype("object")

    for i in range(0, 131):
        object_ids = np.full(
            131, fill_value=object_frame["id"][i], dtype=np.int64
        ).tolist()
        source_frame.at[i, "object_id"] = object_ids
    source_frame = source_frame.explode(["object_id"])

    bands = np.random.randint(6, size=131 * 131)
    # bands = [x for x in bands]
    bands = ["ugrizy"[x] for x in bands]
    source_frame["band"] = bands

    mjds = np.random.rand(131 * 131)
    mjds = [58363 + 1200 * x for x in mjds]
    source_frame["mjd"] = mjds

    mags = np.random.rand(131 * 131)
    mags = [15 + 6 * x for x in mags]
    source_frame["mag"] = mags

    ras = np.random.rand(131, 131)
    for i in range(0, 131):
        ras[i] = [object_frame["ra"][i] + 0.6 * x for x in ras[i]]
    ras = ras.reshape((131 * 131))
    source_frame["source_ra"] = ras

    decs = np.random.rand(131, 131)
    for i in range(0, 131):
        decs[i] = [object_frame["dec"][i] + 0.6 * x for x in decs[i]]
    decs = decs.reshape((131 * 131))
    source_frame["source_dec"] = decs

    source_frame = source_frame.sort_values(["mjd", "id"])
    source_frame["source_id"] = np.arange(70_000, 70_000 + 131 * 131)
    source_frame = source_frame[
        [
            "source_id",
            "source_ra",
            "source_dec",
            "mjd",
            "mag",
            "band",
            "object_id",
            "object_ra",
            "object_dec",
        ]
    ]

    ## write it out
    source_frame.to_csv(
        "/home/delucchi/git/hipscat-import/tests/"
        "hipscat_import/data/small_sky/small_sky_source.csv",
        index=False,
    )

    e = time.time()
    print(f"Elapsed Time: {e-s}")
