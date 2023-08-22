import glob
import re
import pandas as pd
from tqdm import tqdm
from astropy.table import Table
from astropy.table.table import descr

def convert():
    files = glob.glob("/ocean/projects/phy210048p/shared/hipscat/raw/sdss/calibObj-000094**.fits.gz")
    files.sort()

    for in_file in tqdm(files, bar_format='{l_bar}{bar:80}{r_bar}'):

        match = re.match(r".*(calibObj-.*-star).fits.gz", str(file))
        file_prefix = match.group(1)
        out_file = f"/ocean/projects/phy210048p/shared/hipscat/raw/sdss_parquet/{file_prefix}.parquet"

        table = Table.read(in_file)
        new_table = Table()

        for col in table.columns.values():
            descriptor = descr(col)
            col_name = descriptor[0]
            col_shape = descriptor[2]
            if col_shape == (5,):
                data_t = col.data.T
                for index, band_char in enumerate('ugriz'):
                    new_table.add_column(data_t[index], name=f"{col_name}_{band_char}")
            elif col_shape == ():
                new_table.add_column(col)

        new_table.to_pandas().to_parquet(out_file)

if __name__ == "__main__":
    convert()
    # read_fits()
