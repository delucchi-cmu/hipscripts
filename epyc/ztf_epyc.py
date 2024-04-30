"""Main method to enable command line execution

ztf schema:
required group field_id=-1 schema {
  optional int64 field_id=-1 ps1_objid;
  optional double field_id=-1 ra;
  optional double field_id=-1 dec;
  optional binary field_id=-1 filterName (String);
  optional float field_id=-1 midPointTai;
  optional float field_id=-1 psFlux;
  optional float field_id=-1 psFluxErr;
  optional int64 field_id=-1 __index_level_0__;
}

>> python3 ztf_epyc.py &> log.txt
"""

import hipscat_import.dask_map_reduce as runner
import pandas as pd
from hipscat_import.arguments import ImportArguments


def filter_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where dup is true"""
    return data[data.dup == 0]


if __name__ == "__main__":
    args = ImportArguments()
    args.from_params(
        catalog_name="ztf_dr14",
        input_path="/data3/epyc/data3/hipscat/ztf_shards/",
        input_format="parquet",
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=6,
        filter_function=filter_duplicates,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/",
    )
    runner.run(args)
