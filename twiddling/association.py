"""Create columnar index of hipscat table using dask for parallelization

Methods in this file set up a dask pipeline using futures. 
The actual logic of the map reduce is in the `map_reduce.py` file.
"""

import os
import shutil
import time

import dask.dataframe as dd
import pandas as pd
import pyarrow
import pyarrow.dataset
import pyarrow.parquet as pq


def create_association():
    """do stuff."""

    index_dir = "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/association"

    shutil.rmtree(index_dir, ignore_errors=True)

    primary_index = dd.read_parquet(
        path=("/home/delucchi/git/hipscat-import/tests/hipscat_import/data/" "small_sky_object_catalog/"),
        columns=["id", "_hipscat_index", "Norder", "Dir", "Npix"],
        dataset={"partitioning": "hive"},
    )
    primary_index = primary_index.rename(
        columns={"id": "primary_id", "_hipscat_index": "primary_hipscat_index"}
    ).set_index("primary_id")

    print("--init primary index")
    join_index = dd.read_parquet(
        path=("/home/delucchi/git/hipscat-import/tests/hipscat_import/data/" "small_sky_source_catalog/"),
        columns=["object_id", "source_id", "_hipscat_index", "Norder", "Dir", "Npix"],
        dataset={"partitioning": "hive"},
    )
    join_index = join_index.rename(
        columns={
            "source_id": "join_id",
            "_hipscat_index": "join_hipscat_index",
            "Norder": "join_Norder",
            "Dir": "join_Dir",
            "Npix": "join_Npix",
        }
    ).set_index("object_id")

    print("--init join index")
    join_data = primary_index.merge(join_index, how="inner", left_index=True, right_on="object_id")

    print("--init join data")
    join_data = join_data.reset_index()
    join_data = join_data.rename(columns={"object_id": "primary_id"})

    intermediate_dir = f"{index_dir}/intermediate/"
    os.makedirs(intermediate_dir, exist_ok=True)

    groups = (
        join_data.groupby(["Norder", "Dir", "Npix", "join_Norder", "join_Dir", "join_Npix"]).count().compute()
    )
    groups.to_csv(f"{intermediate_dir}/partitions.csv")

    join_data.to_parquet(
        path=intermediate_dir,
        engine="pyarrow",
        partition_on=["Norder", "Dir", "Npix", "join_Norder", "join_Dir", "join_Npix"],
        compute_kwargs={"partition_size": 1_000_000_000},
        write_index=True,
    )
    print("--wrote join data")


def create_catalog():
    """clean up the data"""

    index_dir = "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/association"
    intermediate_dir = f"{index_dir}/intermediate/"
    data_frame = pd.read_csv(f"{intermediate_dir}/partitions.csv")
    data_frame = data_frame[data_frame["primary_hipscat_index"] != 0]
    data_frame["num_rows"] = data_frame["primary_hipscat_index"]
    data_frame = data_frame[["Norder", "Dir", "Npix", "join_Norder", "join_Dir", "join_Npix", "num_rows"]]
    data_frame.to_csv(f"{index_dir}/partitions.csv", index=False)
    print("--wrote partition info")

    for _, partition in data_frame.iterrows():
        input_dir = os.path.join(
            intermediate_dir,
            f'Norder={partition["Norder"]}',
            f'Dir={partition["Dir"]}',
            f'Npix={partition["Npix"]}',
            f'join_Norder={partition["join_Norder"]}',
            f'join_Dir={partition["join_Dir"]}',
            f'join_Npix={partition["join_Npix"]}',
        )
        output_dir = os.path.join(
            index_dir,
            f'Norder={partition["Norder"]}',
            f'Dir={partition["Dir"]}',
            f'Npix={partition["Npix"]}',
            f'join_Norder={partition["join_Norder"]}',
            f'join_Dir={partition["join_Dir"]}',
        )
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f'join_Npix={partition["join_Npix"]}.parquet')
        table = pq.read_table(input_dir)
        rows_written = len(table)

        if rows_written != partition["num_rows"]:
            raise ValueError(
                "Unexpected number of objects ",
                f" Expected {partition['num_rows']}, wrote {rows_written}",
            )

        pq.write_table(table, where=output_file)

    shutil.rmtree(intermediate_dir, ignore_errors=True)


if __name__ == "__main__":
    start = time.perf_counter()

    create_association()
    create_catalog()

    end = time.perf_counter()
    print(f"Elapsed time: {int(end-start)} sec")
