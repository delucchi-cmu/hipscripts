
import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader, InputReader
from dask.distributed import Client
import pyarrow.parquet as pq
import pyarrow as pa
import re

class BandParquetReader(ParquetReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def read(self, input_file):
        match = re.match(r'.*ztf_[\d]+_[\d]+_([gir]).parquet', str(input_file))
        band = match.group(1)
        parquet_file = pq.read_table(input_file, **self.kwargs)
        for smaller_table in parquet_file.to_batches(max_chunksize=self.chunksize):
            frame = pa.Table.from_batches([smaller_table]).to_pandas()
            frame["band"] = band
            yield frame

def import_sources(client):
    args = ImportArguments(
        output_catalog_name="zubercal_3215",
        input_path="/epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/F3215/",
        file_reader=BandParquetReader(),
        input_format="parquet",
        catalog_type="source",
        ra_column="objra",
        dec_column="objdec",
        id_column="objectid",
        pixel_threshold=10_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/zubercal/",
        overwrite=True,
        highest_healpix_order=10,
        resume=True,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/zubercal/",
        output_path="/data3/epyc/data3/hipscat/catalogs/zuber_test",
    )
    runner.pipeline_with_client(args, client)

if __name__ == "__main__":

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=10,
        threads_per_worker=1,
    ) as client:
        import_sources(client)

    # reader = BandParquetReader()

    # next(reader.read("/epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/F3215/ztf_3215_1780_g.parquet"))

