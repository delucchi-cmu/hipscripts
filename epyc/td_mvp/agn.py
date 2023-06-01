"""Main method to enable command line execution

"""

import tempfile

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import fits
from astropy.table import Table
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import FitsReader


def do_import():

    filename = "/data3/epyc/data3/hipscat/raw/dr16q/dr16q_prop_Oct23_2022.fits.gz"
    multidimensional = [
        "CONTI_FIT",
        "CONTI_FIT_ERR",
        "CONTI_STAT",
        "FEII_UV",
        "FEII_UV_ERR",
        "FEII_OPT",
        "FEII_OPT_ERR",
        "HALPHA",
        "HALPHA_BR",
        "NII6585",
        "SII6718",
        "HBETA",
        "HBETA_BR",
        "HEII4687",
        "HEII4687_BR",
        "OIII5007",
        "OIII5007C",
        "CAII3934",
        "OII3728",
        "NEV3426",
        "MGII",
        "MGII_BR",
        "CIII_ALL",
        "CIII_BR",
        "SIIII1892",
        "ALIII1857",
        "NIII1750",
        "CIV",
        "HEII1640",
        "HEII1640_BR",
        "SIIV_OIV",
        "OI1304",
        "LYA",
        "NV1240",
        "HALPHA_ERR",
        "HALPHA_BR_ERR",
        "NII6585_ERR",
        "SII6718_ERR",
        "HBETA_ERR",
        "HBETA_BR_ERR",
        "HEII4687_ERR",
        "HEII4687_BR_ERR",
        "OIII5007_ERR",
        "OIII5007C_ERR",
        "CAII3934_ERR",
        "OII3728_ERR",
        "NEV3426_ERR",
        "MGII_ERR",
        "MGII_BR_ERR",
        "CIII_ALL_ERR",
        "CIII_BR_ERR",
        "SIIII1892_ERR",
        "ALIII1857_ERR",
        "NIII1750_ERR",
        "CIV_ERR",
        "HEII1640_ERR",
        "HEII1640_BR_ERR",
        "SIIV_OIV_ERR",
        "OI1304_ERR",
        "LYA_ERR",
        "NV1240_ERR",
        "HA_COMP_STAT",
        "HB_COMP_STAT",
        "MGII_COMP_STAT",
        "CIII_COMP_STAT",
        "CIV_COMP_STAT",
        "SIIV_COMP_STAT",
        "LYA_COMP_STAT",
        "CAII_LOC_STAT",
        "OII_LOC_STAT",
        "NEV_LOC_STAT",
        "ZSYS_LINES",
        "ZSYS_LINES_ERR",
    ]
    with tempfile.TemporaryDirectory() as tmp_dir:
        args = ImportArguments(
            output_catalog_name="dr16q_constant",
            input_file_list=[filename],
            file_reader=FitsReader(
                chunksize=50_000,
                skip_column_names=multidimensional,
            ),
            input_format="fits",
            ra_column="RA",
            dec_column="DEC",
            id_column="SDSS_NAME",
            pixel_threshold=100_000,
            tmp_dir=tmp_dir,
            overwrite=True,
            constant_healpix_order=5,
            dask_n_workers=1,
            dask_threads_per_worker=1,
            dask_tmp=tmp_dir,
            output_path="/data3/epyc/data3/hipscat/catalogs/",
        )
        runner.run(args)


def read_fits():
    """Make sure we can read the file at all."""
    filename = "/data3/epyc/data3/hipscat/raw/dr16q/dr16q_prop_Oct23_2022.fits.gz"
    multidimensional = [
        "CONTI_FIT",
        "CONTI_FIT_ERR",
        "CONTI_STAT",
        "FEII_UV",
        "FEII_UV_ERR",
        "FEII_OPT",
        "FEII_OPT_ERR",
        "HALPHA",
        "HALPHA_BR",
        "NII6585",
        "SII6718",
        "HBETA",
        "HBETA_BR",
        "HEII4687",
        "HEII4687_BR",
        "OIII5007",
        "OIII5007C",
        "CAII3934",
        "OII3728",
        "NEV3426",
        "MGII",
        "MGII_BR",
        "CIII_ALL",
        "CIII_BR",
        "SIIII1892",
        "ALIII1857",
        "NIII1750",
        "CIV",
        "HEII1640",
        "HEII1640_BR",
        "SIIV_OIV",
        "OI1304",
        "LYA",
        "NV1240",
        "HALPHA_ERR",
        "HALPHA_BR_ERR",
        "NII6585_ERR",
        "SII6718_ERR",
        "HBETA_ERR",
        "HBETA_BR_ERR",
        "HEII4687_ERR",
        "HEII4687_BR_ERR",
        "OIII5007_ERR",
        "OIII5007C_ERR",
        "CAII3934_ERR",
        "OII3728_ERR",
        "NEV3426_ERR",
        "MGII_ERR",
        "MGII_BR_ERR",
        "CIII_ALL_ERR",
        "CIII_BR_ERR",
        "SIIII1892_ERR",
        "ALIII1857_ERR",
        "NIII1750_ERR",
        "CIV_ERR",
        "HEII1640_ERR",
        "HEII1640_BR_ERR",
        "SIIV_OIV_ERR",
        "OI1304_ERR",
        "LYA_ERR",
        "NV1240_ERR",
        "HA_COMP_STAT",
        "HB_COMP_STAT",
        "MGII_COMP_STAT",
        "CIII_COMP_STAT",
        "CIV_COMP_STAT",
        "SIIV_COMP_STAT",
        "LYA_COMP_STAT",
        "CAII_LOC_STAT",
        "OII_LOC_STAT",
        "NEV_LOC_STAT",
        "ZSYS_LINES",
        "ZSYS_LINES_ERR",
    ]

    chunksize = 50_000

    table = Table.read(filename, memmap=True)
    table.remove_columns(multidimensional)

    total_rows = len(table)
    read_rows = 0

    while read_rows < total_rows:
        print(len(table[read_rows : read_rows + chunksize].to_pandas()))
        read_rows += chunksize
        print(read_rows)


if __name__ == "__main__":
    do_import()
    # read_fits()
