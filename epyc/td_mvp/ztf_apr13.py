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

import pandas as pd

import hipscat_import.catalog.run_import as runner
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hipscat_import.catalog.arguments import ImportArguments
from hipscat_import.catalog.file_readers import ParquetReader
from dask.distributed import Client



#### -----------------
## Columns that will be repeated per object
REPEATED_COLUMNS = [
    "ps1_objid", "ra", "dec", "ps1_gMeanPSFMag", "ps1_rMeanPSFMag", "ps1_iMeanPSFMag",
    "nobs_g", "nobs_r", "nobs_i","mean_mag_g","mean_mag_r","mean_mag_i"]

def import_objects(client):
    args = ImportArguments(
        catalog_name="ztf_dr14",
        input_file_list=["/data3/epyc/data3/hipscat/catalogs/ztf_dr14/Norder=1/Dir=0/Npix=33.parquet"],
        file_reader=ParquetReader(
        chunksize=50_000,
        columns=REPEATED_COLUMNS,
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=6,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr13/",
    )
    runner.run_with_client(args, client=client)

def transform_sources(data: pd.DataFrame) -> pd.DataFrame:
    """Explode repeating detections"""

    ## band-specific columns to timedomain_columns
    g_column_map = {"catflags_g":"catflags",
                            "fieldID_g" : "fieldID", 
                            "mag_g":"mag",
                            "magerr_g":"magerr",
                            "mjd_g":"mjd", 
                            "rcID_g":"rcID"}
    g_columns = list(g_column_map.keys())
    r_column_map = {"catflags_r":"catflags",
                            "fieldID_r" : "fieldID", 
                            "mag_r":"mag",
                            "magerr_r":"magerr",
                            "mjd_r":"mjd", 
                            "rcID_r":"rcID"}
    r_columns = list(r_column_map.keys())
    i_column_map = {"catflags_i":"catflags",
                            "fieldID_i" : "fieldID", 
                            "mag_i":"mag",
                            "magerr_i":"magerr",
                            "mjd_i":"mjd", 
                            "rcID_i":"rcID"}
    i_columns = list(i_column_map.keys())

    explode_columns = list(g_column_map.values())

    just_i = data[REPEATED_COLUMNS+i_columns]
    just_i = just_i.rename(columns=i_column_map)
    just_i['band'] = 'i'

    just_g = data[REPEATED_COLUMNS+g_columns]
    just_g = just_g.rename(columns=g_column_map)
    just_g['band'] = 'g'

    just_r = data[REPEATED_COLUMNS+r_columns]
    just_r = just_r.rename(columns=r_column_map)
    just_r['band'] = 'r'

    explodey = pd.concat([just_i, just_g, just_r]).explode(explode_columns)
    explodey = explodey[explodey['mag'].notna()]
    explodey = explodey.sort_values(["ps1_objid", 'band', "mjd"])
    print("explodey size (nan-filtered)", len(explodey))

    explodey = explodey.reset_index()

    return explodey

def import_sources(client):
    args = ImportArguments(
        catalog_name="ztf_source",
        input_file_list=["/data3/epyc/data3/hipscat/catalogs/ztf_dr14/Norder=1/Dir=0/Npix=33.parquet"],
        file_reader=ParquetReader(
            chunksize=100_000
        ),
        ra_column="ra",
        dec_column="dec",
        id_column="ps1_objid",
        pixel_threshold=1_500_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
        highest_healpix_order=7,
        filter_function=transform_sources,
        # debug_stats_only=True,
        # progress_bar=False,
        resume=False,
        dask_n_workers=10,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr13/",
    )
    runner.run_with_client(args, client=client)

import hipscat_import.association.run_association as a_runner
from hipscat_import.association.arguments import AssociationArguments

def create_association():
    args = AssociationArguments(
        primary_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr13/ztf_dr14",
        primary_id_column="ps1_objid",
        primary_join_column="ps1_objid",
        join_input_catalog_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr13/ztf_source",
        join_id_column="ps1_objid",
        join_foreign_key="ps1_objid",
        output_path="/data3/epyc/data3/hipscat/catalogs/ztf_apr13/",
        output_catalog_name="ztf_object_to_source",
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        overwrite=True,
    )
    a_runner.run_with_client(args)

if __name__ == "__main__":

    with Client(
        local_directory="/data3/epyc/data3/hipscat/tmp/",
        n_workers=10,
        threads_per_worker=10,
    ) as client:
        # import_objects(client)
        # import_sources(client)
        create_association()
