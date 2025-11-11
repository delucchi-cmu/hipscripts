from pyarrow import csv
import pyarrow as pa
import pyarrow.parquet as pq
from hats_import.catalog.file_readers import InputReader

class CsvPyarrowReader(InputReader):
    
    def __init__(self, column_names=None, convert_options=None,read_options=None, **kwargs):
        self.kwargs = kwargs
        self.column_names = column_names
        self.convert_options = convert_options or csv.ConvertOptions()
        self.read_options = read_options or csv.ReadOptions(block_size=1048576*10)

        
    def read(self, input_file, read_columns=None):
        self.regular_file_exists(input_file, **self.kwargs)

        read_columns = read_columns or self.column_names
        convert_options = self.convert_options
        print(convert_options)
        if read_columns:
            convert_options.include_columns = read_columns
        with csv.open_csv(input_file, convert_options=convert_options, read_options=self.read_options, **self.kwargs) as reader:
            for next_chunk in reader:
                if next_chunk is None:
                    break
                table = pa.Table.from_batches([next_chunk])
                table = table.replace_schema_metadata()
                yield table
