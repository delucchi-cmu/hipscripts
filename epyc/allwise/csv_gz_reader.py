import abc
from typing import Any, Dict, Union

import pandas as pd
import pyarrow
import pyarrow.dataset
import pyarrow.parquet as pq
from astropy.io import ascii as ascii_reader
from astropy.table import Table
from hipscat.io import FilePointer, file_io
from hipscat_import.catalog.file_readers import InputReader

class CsvGzReader(InputReader):
    """CSV reader for the most common CSV reading arguments.

    This uses `pandas.read_csv`, and you can find more information on
    additional arguments in the pandas documentation:
    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html

    Attributes:
        chunksize (int): number of rows to read in a single iteration.
        header (int, list of int, None, default 'infer'): rows to
            use as the header with column names
        schema_file (str): path to a parquet schema file. if provided, header names
            and column types will be pulled from the parquet schema metadata.
        column_names (list[str]): the names of columns if no header is available
        type_map (dict): the data types to use for columns
        parquet_kwargs (dict): additional keyword arguments to use when
            reading the parquet schema metadata, passed to pandas.read_parquet.
            See https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
        kwargs (dict): additional keyword arguments to use when reading
            the CSV files with pandas.read_csv.
            See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    """

    def __init__(
        self,
        chunksize=500_000,
        header="infer",
        schema_file=None,
        column_names=None,
        type_map=None,
        parquet_kwargs=None,
        **kwargs,
    ):
        self.chunksize = chunksize
        self.header = header
        self.schema_file = schema_file
        self.column_names = column_names
        self.type_map = type_map
        self.parquet_kwargs = parquet_kwargs
        self.kwargs = kwargs

        schema_parquet = None
        if self.schema_file:
            if self.parquet_kwargs is None:
                self.parquet_kwargs = {}
            schema_parquet = file_io.read_parquet_file_to_pandas(
                FilePointer(self.schema_file),
                **self.parquet_kwargs,
            )

        if self.column_names:
            self.kwargs["names"] = self.column_names
        elif not self.header and schema_parquet is not None:
            self.kwargs["names"] = list(schema_parquet.columns)

        if self.type_map:
            self.kwargs["dtype"] = self.type_map
        elif schema_parquet is not None:
            self.kwargs["dtype"] = schema_parquet.dtypes.to_dict()

    def read(self, input_file, read_columns=None):
        self.regular_file_exists(input_file, **self.kwargs)

        if read_columns:
            self.kwargs["usecols"] = read_columns

        with pd.read_csv(input_file, chunksize=self.chunksize, header=self.header, **self.kwargs) as reader:
            yield from reader


        # return file_io.load_csv_to_pandas_generator(
        #     FilePointer(input_file),
        #     chunksize=self.chunksize,
        #     header=self.header,
        #     **self.kwargs,
        # )