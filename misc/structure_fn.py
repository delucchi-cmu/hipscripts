import time

from lsstseries import ensemble
from lsstseries.analysis.stetsonj import calc_stetson_J

from catalog.catalog import Catalog

if __name__ == "__main__":
    start = time.perf_counter()

    catalog = Catalog("/home/delucchi/xmatch/catalogs/td_demo")
    print(len(catalog.get_partitions()))

    ens = ensemble()
    # for file in catalog.get_partitions():
    ens.from_parquet(
        catalog.get_partitions()[0],
        id_col="diaObjectId",
        band_col="filterName",
        flux_col="psFlux",
        err_col="psFluxErr",
    )
    result = ens.prune(10).dropna(1).batch(calc_stetson_J, band_to_calc=None)

    end = time.perf_counter()
    print(f"Elapsed time: {int(end-start)} sec")
