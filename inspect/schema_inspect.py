import pandas as pd
import pyarrow.parquet as pq

## You're gonna want to change this file name!!

# file_name = "/home/delucchi/tic/tic_schema.parquet"
# file_name = "/home/delucchi/td_data/base/truth_tract3830.parquet"
# file_name = "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/small_sky_object_index/intermediate/index_1_44.parquet"
file_name = "/home/delucchi/git/hipscat-import/tests/hipscat_import/data/association/part.0.parquet"
parquet_file = pq.ParquetFile(file_name)
print(parquet_file.metadata)
print(parquet_file.schema)
