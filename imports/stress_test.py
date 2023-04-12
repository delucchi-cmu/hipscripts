"""Main method to enable command line execution

"""

import tempfile

import hipscat_import.run_import as runner
from hipscat_import.arguments import ImportArguments

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        args = ImportArguments()
        args.from_params(
            catalog_name="ztf_dr14",
            # input_file_list=[
            #     "/home/delucchi/td_data/base/truth_tract3830.parquet",
            # #     "/data/epyc/projects/lsd2/pzwarehouse/ztf_dr14/part-00498-dd7ffb0e-fa6a-471d-9498-31699eaece0d_00498.c000.snappy.parquet",
            # #     # "/data/epyc/projects/lsd2/pzwarehouse/ztf_dr14/part-00499-dd7ffb0e-fa6a-471d-9498-31699eaece0d_00499.c000.snappy.parquet",
            # ],
            input_path="/home/delucchi/td_data/base/",
            input_format="parquet",
            ra_column="ra",
            dec_column="dec",
            id_column="id",
            pixel_threshold=1_000_000,
            tmp_dir=tmp_dir,
            overwrite=True,
            highest_healpix_order=10,
            progress_bar=False,
            dask_n_workers=2,
            dask_threads_per_worker=1,
            dask_tmp=tmp_dir,
            output_path="/home/delucchi/xmatch/catalogs/",
        )
        runner.run(args)
