import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader, InputReader
from dask.distributed import Client
import pyarrow.parquet as pq
import pyarrow as pa
import re
import glob


class ZubercalParquetReader(ParquetReader):
    def read(self, input_file):
        """Reader for the specifics of zubercal parquet files.

        - the ``__index_level_0__`` pandas index should be ignored when reading
            - it is identical to the ``objectid`` column, and is just bloat
            - it is non-unique, and that makes it tricky when splitting the file up
        - the files are written out by band, and the band is included in the file
          name (but not as a field in the resulting data product). this uses a 
          regular expression to parse out the band and insert it as a column in
          the dataframe.
        - the parquet files are all a fine size for input files, so we don't mess
          with another chunk size.
        """
        columns = [
            "mjd",
            "mag",
            "objdec",
            "objra",
            "magerr",
            "objectid",
            "info",
            "flag",
            "rcidin",
            "fieldid",
        ]

        match = re.match(r".*ztf_[\d]+_[\d]+_([gir]).parquet", str(input_file))
        band = match.group(1)
        parquet_file = pq.read_table(input_file, columns=columns, **self.kwargs)
        for smaller_table in parquet_file.to_batches(max_chunksize=self.chunksize):
            frame = pa.Table.from_batches([smaller_table]).to_pandas()
            frame["band"] = band
            yield frame


def import_sources():

    files = glob.glob("/epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/**/**.parquet")
    files.sort()

    args = ImportArguments(
        output_catalog_name="zubercal",
        input_file_list=files,
        ## NB - you need the parens here!
        file_reader=ZubercalParquetReader(),
        input_format="parquet",
        catalog_type="source",
        ra_column="objra",
        dec_column="objdec",
        id_column="objectid",
        highest_healpix_order=10,
        pixel_threshold=20_000_000,
        output_path="/data3/epyc/data3/hipscat/catalogs/",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/zubercal/",
    )


    import ray
    from ray.util.dask import enable_dask_on_ray, disable_dask_on_ray

    context = ray.init(
        num_cpus=30,
        _temp_dir="/data3/epyc/projects3/mmd11/ray",
    )

    enable_dask_on_ray()

    # do any dask stuff and it will run on ray

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=30,
        threads_per_worker=1,
    ) as client:
        runner.pipeline_with_client(args, client)


if __name__ == "__main__":
    import_sources()
