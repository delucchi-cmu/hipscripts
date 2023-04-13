"""Main method to enable command line execution"""

import hipscat_import.dask_map_reduce as runner
from hipscat_import.arguments import ImportArguments


if __name__ == "__main__":
    args = ImportArguments()
    args.from_params(
        catalog_name="ps1_skinny",
        input_path="/data3/epyc/data3/hipscat/ps_shards/",
        input_format="parquet",
        ra_column="raMean",
        dec_column="decMean",
        id_column="objID",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=6,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
    )
    runner.run(args)
