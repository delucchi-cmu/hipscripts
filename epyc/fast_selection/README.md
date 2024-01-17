# fast selection stuff

https://github.com/astronomy-commons/lsdb/issues/108

## index generation

Ok. What am I trying to do?

Create an index for a catalog? ZTF? Which one?

Looks like AndyT is using both gaia and ZTF for things. But probably just ZTF for timedomain.

https://github.com/dirac-institute/ZTF_FG_BoyajianSearch/blob/main/analysis/notebooks/tda-uw-demo/time-series%20open-clusters.ipynb

```python
#Load ZTF, Gaia, and ZTF sources hipscats
gaia = lsdb.read_hipscat("/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic")

# load ZTF object table
ztf = lsdb.read_hipscat("/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_dr14")
```

So. ID index over ztfdr14? I'm not having a good focus day. And getting
my conda environment updated is taking sooooo long.