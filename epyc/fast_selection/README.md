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

Well, it finally ran. Mostly by just not attempting to de-duplicate.

    real    22m6.364s
    user    164m8.704s
    sys     83m26.574s

That's pretty good timing! And the output is ~15G, which is pretty reasonable.
The individual files are freaking tiny, though. Some are < 1M. Most are 3-4 M.
Maybe I'll try upping the compute partition size?

The thing that's most annoying now is that the progress bar doesn't show up AT ALL.

    real    20m58.673s
    user    154m37.138s
    sys     71m47.616s

Hm. Still 7500 partitions, that are the same sizes. So the partition size
maybe doesn't do anything? Or there's some secret max?

LOLOL. The secret max is the divisions I set in the `divisions` hint parameter.
Ya know. The thing Sandro warned about in his code review. Let's change that.

Boom. Roasted. Yeah. Down to 45 files. And it's 14G instead of 15G =D

Now let's use that in a fast selection use case and see what happens!

It's about 40 seconds to do the `loc_partitions`, but once you get that, 
it's like 10s for the index search bits.

So I think putting some effort into speeding that up would be a good idea!