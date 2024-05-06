# dask / dask-expr reproducer

See https://github.com/dask/dask-expr/issues/777#issuecomment-2080460481

Can I make some kinda reproducer?

Simple code snippet.py:

```
import numpy as np
import dask.dataframe as dd
import pandas as pd

if __name__ == "__main__":
    a = dd.from_dict({"a": np.arange(300_000)}, npartitions=30_000)
    parts = a.to_delayed()
    dd.from_delayed(
        parts[0], meta=pd.DataFrame.from_dict({"a": pd.Series(dtype=np.int64)})
    ).compute()
```

>> pip install numpy pandas 'dask<=2024.2.0'
>> time python snippet.py

real	0m1.094s
user	0m1.435s
sys	    0m1.383s

>> pip install 'dask>=2024.3.1'
>> time python snippet.py

real	0m1.127s
user	0m1.507s
sys	    0m1.360s

>> pip install dask-expr
>> time python snippet.py

real	0m48.497s
user	0m48.845s
sys	0m1.381s

