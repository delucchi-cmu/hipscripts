# Running import.

Forgot to set the distance to 10 arcs. So it ran with 5 arcs. 

    $ time python ztf_margin.py &> margin.log

    real    566m34.663s
    user    4482m57.967s
    sys     1464m18.630s

Maybe I can run again and see if the distance makes much of a difference?
I suspect it won't. But I have to run it again anyway =P

    Mapping  : 100%|██████████| 2352/2352 [9:25:00<00:00, 14.41s/it]
    Reducing : 100%|██████████| 2385/2385 [00:43<00:00, 54.47it/s] 
    Finishing: 100%|██████████| 4/4 [00:03<00:00,  1.15it/s]

## Again, but with 10 arcs.

And with the "Planning " progress bar.

Though the planning is not the expensive part. I think it's the argument creation
and dask client creation. Which don't have anything like a progress bar to
tell you about what's happening.

# 2024-02-09

Looking also at a quick data audit:

```
$ df -h /data*
Filesystem      Size  Used Avail Use% Mounted on
/dev/sda1       146T  133T   14T  91% /data
/dev/sdb1       175T  170T  5.4T  97% /data2
/dev/sdc1       393T  327T   67T  84% /data3
/dev/sdd1       241T   18G  241T   1% /data4
```

So it looks like I might be a bit of a jerk and need to clean up some 
stuff.

Might not be all hipscat? Let's look at that usage in particular.

```
$ du -chs /data3/epyc/data3/hipscat/
111T	/data3/epyc/data3/hipscat/
111T	total
```

Ok. I don't feel nearly as bad now.

But trying to create the margins for `ps1_otmo` has been hung for a day.

Is it just that the notebook isn't updating the progress bar, or is there 
really nothing happening?

Notebook output:

    /astro/users/mmd11/.conda/envs/hipscatenv/lib/python3.10/site-packages/distributed/node.py:182: UserWarning: Port 8787 is already in use.
    Perhaps you already have a cluster running?
    Hosting the HTTP server on port 38591 instead
    Mapping  :  40%|███▉      | 10771/27161 [50:48:45<38:57:35,  8.56s/it] 

Count of intermediate directories:

    $ ls -lR | grep ^d | wc -l
    81654

According to top, dask has been really busy this whole time. But I don't 
know what it's doing. And maybe I should have been looking at a dask
dashboard this whole time. I dunno. I think I might have to just restart 
it, but there's no resume behavior.

Well, a few hours later, the numbers are different.

    $ ls -lR | grep ^d | wc -l
    83948

So maybe I should just wait it out?

Um. Fun. We have to re-import all of GAIA, so the margins are invalid.

Cool. Cool cool cool.