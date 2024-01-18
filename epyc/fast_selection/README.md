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

## Update like 2 weeks later.

Finally got something running that actually finishes. And it's fucking fast!

    real    9m1.468s
    user    54m54.385s
    sys     43m32.002s

The trick is setting some reasonable guess on `divisions`. This requires some prior
knowledge of the column that you're generating your index over.

## Gaia and ArrowCapacityError

I think the divisions are still too large? I tried to make divisions that are comparable to
the distribution of psids (just linear in that weird integer space, and divided by the number
of leaf partitions in the primary gaia catalog). And that results in errors like:

    Exception: "ArrowCapacityError('array cannot contain more than 2147483646 bytes, have 2147483648')"

So I think
1. trying even smaller increment for divisions (limited success)
2. trying to massage the data differently so we're not storing that giant `designation`
    twice for each frame (was both the index and a field value). (we'll see.)