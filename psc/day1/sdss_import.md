# Import SDSS

## Set up conda environment.

Lessee how much of this is necessary to resume work:

### First time

```
$ interact
$ module load anaconda3
$ conda activate
$ conda create -n hipscatenv python=3.10
$ conda activate hipscatenv
$ cd /ocean/projects/phy210048p/shared/hipscat
$ time python /jet/home/delucchi/git/hipscripts/psc/day1/sdss_1file.py
```

### Second time

```
$ interact
$ module load anaconda3
$ conda activate hipscatenv
$ cd /ocean/projects/phy210048p/shared/hipscat
$ time python /jet/home/delucchi/git/hipscripts/psc/day1/sdss_1file.py
```

## Then run some things

Still waiting on 80% of the download, but going to try to run the import anyway.

First, convert some FITS files into flatter parquet files.

Using the `convert()` method, 

```
$ time python /jet/home/delucchi/git/hipscripts/psc/day1/sdss_1file.py
100%|████████████████████████████████████████████████████████████████████████████████| 6/6 [00:26<00:00,  4.34s/it]

real    0m28.543s
user    0m16.570s
sys     0m2.658s
```

And 6 parquet files got spit out.

Now going to try an import. **holds breath**

Ok. that `$LOCAL` thing didn't work as expected:
`FileNotFoundError: tmp_dir ($LOCAL) not found on local storage`.

Easy fix:

```
    local_tmp = os.path.expandvars("$LOCAL")
    args = ImportArguments(
        tmp_dir=local_tmp,
        dask_tmp=local_tmp,
        ...
    )
```

Ugh. Not so easy fix. There's only .98GB of memory per-worker. That's going to suck.

Ok. I can set it higher with `memory_limit='2GB',`, but my workers are still getting
restarted by the nanny.

And the TQDM stage names look terrible. Some are caps, some are not. Opening an issue
because I don't need to fix that today.

But I should be able to turn those steps from above into a batch file!!

## Batch file

Trying out:

`sbatch do_a_thing.batch`

from a login node. Let's see how that goes.