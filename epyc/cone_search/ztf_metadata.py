from hipscat.catalog.catalog import Catalog
from hipscat.catalog.partition_info import  PartitionInfo
from hipscat.io.parquet_metadata import write_parquet_metadata
import time
import os

def do_stuff1():
    start = time.perf_counter()
    print("starting")

    catalog_path = "/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14"

    partition_info = PartitionInfo.read_from_csv(os.path.join(catalog_path, "partition_info.csv"))
    print("num partitions:", len(partition_info.get_healpix_pixels()))

    end = time.perf_counter()
    print(f'finished task in {int(end-start)} s')
    start = end

    partition_info2 = PartitionInfo.read_from_file(os.path.join(catalog_path, "_metadata"))
    print("num partitions:", len(partition_info2.get_healpix_pixels()))
    end = time.perf_counter()
    print(f'finished task in {int(end-start)} s')
    start = end

    set1 = set(partition_info.get_healpix_pixels())
    set2 = set(partition_info2.get_healpix_pixels())

    print(set2.difference(set1))

    end = time.perf_counter()
    print(f'finished task in {int(end-start)} s')
    start = end

def do_stuff2():
    start = time.perf_counter()
    print("starting")
    catalog_path = "/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14"

    write_parquet_metadata(catalog_path)
    end = time.perf_counter()
    print(f'finished task in {int(end-start)} s')
    start = end
    catalog = Catalog.read_from_hipscat(catalog_path)
    end = time.perf_counter()
    print(f'finished task in {int(end-start)} s')
    start = end



if __name__ == "__main__":
    # do_stuff1()

    ## in between:
    ## rm -rf /data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14/Norder=0/
    do_stuff2()