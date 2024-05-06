"""
detailedCatalog_matchDec50_00N__70_00N.csv
SumMagAper2.csv
SumMagAuto.csv
SumPropMagAper2Cat.csv
SumPropMagAutoCat.csv
detailedCatalog_matchDec10_00N__10_00N.csv
detailedCatalog_matchDec10_00N__30_00N.csv
detailedCatalog_matchDec30_00N__50_00N.csv
detailedCatalog_matchDec30_00S__10_00S.csv
detailedCatalog_matchDec50_00S__30_00S.csv
detailedCatalog_matchDec70_00S__50_00S.csv
detailedCatalog_matchDec90_00S__70_00S.csv
detailedCatalog_matchDec70_00N__90_00N.csv
"""

"""Main method to enable command line execution
"""

import hipscat_import.run_import as runner
import pandas as pd
from hipscat_import.arguments import ImportArguments
from hipscat_import.file_readers import CsvReader

if __name__ == "__main__":

    type_frame = pd.read_csv("hst_sum_prop_mag_types.csv")
    type_map = dict(zip(type_frame["name"], type_frame["type"]))

    names = type_frame["name"].values.tolist()
    names.remove("StartTime")
    names.remove("StopTime")

    args = ImportArguments(
        catalog_name="hst_sum_prop_mag_auto",
        input_file_list=["/data3/epyc/data3/hipscat/raw/hst/SumPropMagAutoCat.csv"],
        input_format="csv",
        file_reader=CsvReader(
            header=None,
            column_names=type_frame["name"].values.tolist(),
            type_map=type_map,
            chunksize=500_000,
            usecols=names,
        ),
        ra_column="MatchRA",
        dec_column="MatchDec",
        id_column="MatchID",
        pixel_threshold=1_000_000,
        tmp_dir="/data3/epyc/data3/hipscat/tmp/",
        # overwrite=True,
        # resume=True,
        highest_healpix_order=10,
        # debug_stats_only=True,
        # progress_bar=False,
        dask_n_workers=1,
        dask_tmp="/data3/epyc/data3/hipscat/tmp/",
        output_path="/data3/epyc/data3/hipscat/catalogs/stats_only/hst/",
    )
    runner.run(args)
