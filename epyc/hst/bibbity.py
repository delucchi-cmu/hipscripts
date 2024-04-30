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

    type_frame = pd.read_csv("hst_detailed_types.csv")
    type_map = dict(zip(type_frame["name"], type_frame["type"]))

    names = type_frame["name"].values.tolist()
    print("all names", len(names))
    # names = names -["MatchRA", "MatchDec", "SourceID"]
    names.remove("StartTime")
    names.remove("StopTime")
    print("all names", len(names))
