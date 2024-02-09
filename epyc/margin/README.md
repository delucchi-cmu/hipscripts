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