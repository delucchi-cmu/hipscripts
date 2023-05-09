"""Main method to enable command line execution
"""

import hipscat_import.dask_map_reduce as runner
from hipscat_import.arguments import ImportArguments

if __name__ == "__main__":
    args = ImportArguments()
    args.from_params(
        catalog_name="allwise",
        input_path="/data3/epyc/data3/hipscat/allwise_shards/",
        input_format="parquet",
        ra_column="ra",
        dec_column="dec",
        id_column="source_id",
        pixel_threshold=250_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=5,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=4,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
    )
    runner.run(args)
