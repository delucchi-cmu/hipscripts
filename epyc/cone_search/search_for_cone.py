import os
import time

from hipscat.catalog.catalog import Catalog
from hipscat.catalog.partition_info import PartitionInfo
from hipscat.io.parquet_metadata import write_parquet_metadata


def do_stuff():
    start = time.perf_counter()
    print("starting")

    # catalog_path = "/data3/epyc/data3/hipscat/catalogs/neowise_yr8"
    ## 393 s

    catalog_path = "/data3/epyc/data3/hipscat/catalogs/dr16q_constant"
    ## 21s (3657 partitions)

    # catalog_path = "/epyc/projects3/sam_hipscat/catwise2020/catwise2020/"
    ## 103 s (4080 partitions)

    # catalog_path = "/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic"
    # write_parquet_metadata(catalog_path="/epyc/projects3/sam_hipscat/gaia/catalog/",
    #                        order_by_healpix=True,  output_path= "/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic")

    partition_info = PartitionInfo.read_from_csv(os.path.join(catalog_path, "partition_info.csv"))
    # catalog = Catalog.read_from_hipscat(catalog_path)
    print("num partitions:", len(partition_info.get_healpix_pixels()))

    end = time.perf_counter()
    print(f"finished task in {int(end-start)} s")
    start = end

    ra = 2  # degrees
    dec = -80  # degrees
    radius = 2  # degrees

    partition_info = PartitionInfo.read_from_file(os.path.join(catalog_path, "_metadata"))
    # catalog = Catalog.read_from_hipscat(catalog_path)
    print("num partitions:", len(partition_info.get_healpix_pixels()))
    # filtered_catalog = catalog.filter_by_cone(ra, dec, radius)
    # print("num partitions in cone:", len(filtered_catalog.partition_info.get_healpix_pixels()))
    # print("all partitions", filtered_catalog.partition_info.get_healpix_pixels())
    end = time.perf_counter()
    print(f"finished task in {int(end-start)} s")
    start = end


if __name__ == "__main__":
    do_stuff()
