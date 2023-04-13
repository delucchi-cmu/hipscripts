"""Main method to enable command line execution"""

import tempfile

import partitioner.dask_runner as dr
from partitioner.arguments import PartitionArguments

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        args = PartitionArguments()
        args.from_params(
            catalog_name="small_sky",
            input_path="/astro/users/mmd11/git/lsdb/tests/partitioner/data/small_sky_parts/",
            input_format="csv",
            output_path="/astro/users/mmd11/catalogs/",
            highest_healpix_order=1,
            ra_column="ra",
            dask_tmp=tmp_dir,
            runtime="dask",
            dec_column="dec",
        )
        dr.run(args)
