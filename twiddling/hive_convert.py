"""Convert an existing catalog directory """

import os
import time

import pandas as pd


def run(in_catalog_path, out_catalog_path):
    """do a thing"""

    ## Get the original partition info
    partition_info_filename = os.path.join(in_catalog_path, "partition_info.csv")
    if not os.path.exists(partition_info_filename):
        raise FileNotFoundError(
            f"No partition info found where expected: {partition_info_filename}"
        )

    original_info = pd.read_csv(partition_info_filename).copy()
    original_info["Dir"] = [int(x / 10_000) * 10_000 for x in original_info["pixel"]]

    ## Rename all of the partition files using hive scheme
    for _, partition in original_info.iterrows():
        norder = int(partition["order"])
        npix = int(partition["pixel"])
        ndir = int(npix / 10_000) * 10_000
        original_path = os.path.join(
            in_catalog_path,
            f"Norder{norder}/Npix{npix}",
            "catalog.parquet",
        )
        new_path = os.path.join(out_catalog_path, f"Norder={norder}/Dir={ndir}")
        new_file = os.path.join(new_path, f"Npix={npix}.parquet")
        os.makedirs(new_path, exist_ok=True)
        if not os.path.exists(original_path):
            raise FileNotFoundError

        os.rename(original_path, new_file)

    ## Format and write new partition info file
    partition_info = (
        original_info.astype(int)
        .rename(
            columns={"order": "Norder", "pixel": "Npix", "num_objects": "row_count"}
        )
        .reindex(["Norder", "Dir", "Npix", "row_count"], axis=1)
    )

    os.makedirs(out_catalog_path, exist_ok=True)
    partition_info_filename = os.path.join(out_catalog_path, "partition_info.csv")
    partition_info.to_csv(partition_info_filename, index=False)

    ## Move catalog info file over, unchanged.
    original_path = os.path.join(in_catalog_path, "catalog_info.json")
    new_file = os.path.join(out_catalog_path, "catalog_info.json")
    os.rename(original_path, new_file)


if __name__ == "__main__":
    s = time.time()
    run(
        "/home/delucchi/xmatch/catalogs/small_sky",
        "/home/delucchi/xmatch/catalogs/small_sky_hive",
    )
