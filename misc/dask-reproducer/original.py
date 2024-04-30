"""
Requirements
>> pip install numpy 'dask<=2024.2.0' pandas

Elapsed time: 5 sec
"""

import numpy as np
import dask.dataframe as dd
import pandas as pd

if __name__ == "__main__":
    a = dd.from_dict({"a": np.arange(300_000)}, npartitions=30_000)
    parts = a.to_delayed()
    dd.from_delayed(
        parts[0], meta=pd.DataFrame.from_dict({"a": pd.Series(dtype=np.int64)})
    ).compute()
