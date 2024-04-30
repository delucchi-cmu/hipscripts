"""Generate a molleview map with the pixel densities of the catalog"""

import time

from hipscat.catalog import Catalog
from hipscat.inspection import visualize_catalog
from matplotlib import pyplot as plt

if __name__ == "__main__":
    start = time.perf_counter()

    catalog = Catalog("/home/delucchi/xmatch/catalogs/agns_const")
    print(len(catalog.get_pixels()))

    visualize_catalog.plot_pixels(catalog, draw_map=True)
    plt.savefig("/home/delucchi/xmatch/catalogs/agns_const/pixel_density.png")

    partition_data = catalog.get_pixels()

    print(f'healpix orders: {partition_data["Norder"].unique()}')
    print(f'num partitions: {len(partition_data["Npix"])}')
    print(f'total rows: {partition_data["num_rows"].sum()}')
    print("------")
    print(f'min rows: {partition_data["num_rows"].min()}')
    print(f'max rows: {partition_data["num_rows"].max()}')
    print(f'object ratio: {partition_data["num_rows"].max()/partition_data["num_rows"].min()}')

    end = time.perf_counter()
    print(f"Elapsed time: {int(end-start)} sec")
