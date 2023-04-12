import glob

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load our CSV of column names and types. Convert it into a map.
type_frame = pd.read_csv("tic_types.csv")

type_map = dict(zip(type_frame["name"], type_frame["type"]))
file_name = "/home/delucchi/tic/tic_dec90_00S__88_00S.csv"
data_frame = pd.read_csv(
    file_name,
    index_col=False,
    sep=",",
    header=None,
    names=type_frame["name"],
    dtype=type_map,
    # verbose=True,
    ## Columns (3,4,20,23,63,76,85,90,103,106,113,114,115) have mixed types.
    ### row 196_608 or 196_609 is the problem
    # chunksize=150_000,
    # skiprows=196_000,
    # nrows=1,
)
# data_frame = data_frame.astype(dtype=type_map)

# data_frame.head(2).transpose().to_csv("/home/delucchi/tic/tic2.csv")
# print(data_frame["splists"])
# print(data_frame.groupby('HIP')['ID'].count())


new_parquet = pa.Table.from_pandas(data_frame).schema.empty_table()
# output_file = "/data3/epyc/data3/hipscat/ps_schema.parquet"
# pq.write_table(new_parquet, where=output_file)


# type_frame = pd.DataFrame(columns=as_frame["name"])
# type_frame = type_frame.astype(dtype=type_map)

# new_parquet = pa.Table.from_pandas(type_frame)
# print(new_parquet.schema)

output_file = "/home/delucchi/tic/tic_schema.parquet"
pq.write_table(new_parquet, where=output_file)
