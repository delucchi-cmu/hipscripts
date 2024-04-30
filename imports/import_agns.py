"""Main method to enable command line execution

"""

import tempfile

import hipscat_import.catalog.run_import as runner
from astropy.table import Table
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import FitsReader

filename_dr16q = "/home/delucchi/td_data/agns/dr16q_prop_Oct23_2022.fits.gz"
multidimensional_dr16q = [
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


filename_dr7 = "/home/delucchi/td_data/agns/dr7_bh_May_2011.fits.gz"
multidimensional_dr7 = [
    "UGRIZ",
    "UGRIZ_ERR",
    "UGRIZ_DERED",
    "JHK",
    "JHK_ERR",
    "WISE1234",
    "WISE1234_ERR",
]


def do_import():
    with tempfile.TemporaryDirectory() as tmp_dir:
        args = ImportArguments(
            output_catalog_name="agns_const",
            input_file_list=[filename_dr16q],
            input_format="fits",
            file_reader=FitsReader(
                chunksize=50_000,
                skip_column_names=multidimensional_dr16q,
            ),
            ra_column="RA",
            dec_column="DEC",
            id_column="SDSS_NAME",
            tmp_dir=tmp_dir,
            overwrite=True,
            constant_healpix_order=3,
            dask_n_workers=1,
            dask_threads_per_worker=1,
            dask_tmp=tmp_dir,
            debug_stats_only=True,
            output_path="/home/delucchi/xmatch/catalogs/",
        )
        runner.run(args)


def read_fits():
    chunksize = 50_000

    table = Table.read(filename_dr16q, memmap=True)
    table.remove_columns(multidimensional_dr16q)

    total_rows = len(table)
    read_rows = 0

    while read_rows < total_rows:
        print(len(table[read_rows : read_rows + chunksize].to_pandas()))
        read_rows += chunksize
        # print(read_rows)


if __name__ == "__main__":
    do_import()
    # read_fits()
