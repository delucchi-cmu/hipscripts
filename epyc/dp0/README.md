# dp0.2 -> hipscat

First, let's get all the files onto epyc. Neven got me a list of files.

wget --content-disposition --trust-server-names -i /astro/users/mmd11/git/scripts/epyc/dp0/files.txt

First file took 2m 5s. I probably should have started this in a screen 
because this is going to take A WHILE. Like 5 hours?

Eeee! I checked on the download when there was like 15 seconds left.

    FINISHED --2024-03-06 10:21:52--
    Total wall clock time: 5h 25m 1s
    Downloaded: 156 files, 998G in 5h 24m 14s (52.5 MB/s)

Tried a single file import. Went smoothly.

Trying to import with all of the files.

Mapping   : 100%|██████████| 157/157 [22:15<00:00,  8.51s/it]
Binning   : 100%|██████████| 2/2 [00:04<00:00,  2.28s/it]
Splitting : 100%|██████████| 157/157 [30:35<00:00, 11.69s/it]
Reducing  : 100%|██████████| 438/438 [24:48<00:00,  3.40s/it]
Finishing :  40%|████      | 2/5 [00:24<00:36, 12.20s/it]

Reducing stage is chock full of these warnings:

  dataframe["Norder"] = np.full(rows_written, fill_value=healpix_pixel.order, dtype=np.uint8)
/astro/users/mmd11/git/hipscat-import/src/hipscat_import/catalog/map_reduce.py:290: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  dataframe["Dir"] = np.full(rows_written, fill_value=healpix_pixel.dir, dtype=np.uint64)
/astro/users/mmd11/git/hipscat-import/src/hipscat_import/catalog/map_reduce.py:291: PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
  dataframe["Npix"] = np.full(rows_written, fill_value=healpix_pixel.pixel, dtype=np.uint64)

And the FINISHING stage fails with the mismatched schema problems.

I think this is because, when reading in all of the files for the reducing stage, 
we just go with whatever order we see first, and fit the remaining shards in.
So each leaf partition is written with the schema ordering of whatever shard was
encountered first. It's only at the stage where we're trying to construct the
`_common_metadata` that this error occurs.

However, all of the leaf files should also be written with a consistent column ordering.

Also, there's information in the schema that doesn't seem to be carrying through?
Or DM isn't including that data?

OK. looking at the parquet files that were sent over, it looks like none of the
description/UCD stuff is included there. should be pretty easy to convert the
reference schema from their website into parquet schema, and re-import using
that schema file.

I should know in a couple hours?

Yup. Just over an hour

    real    72m3.249s
    user    921m53.006s
    sys     423m43.640s

And the breakouts:

    Mapping   : 100%|██████████| 157/157 [09:30<00:00,  3.63s/it]
    Binning   : 100%|██████████| 2/2 [00:04<00:00,  2.06s/it]
    Splitting : 100%|██████████| 157/157 [30:15<00:00, 11.56s/it]
    Reducing  : 100%|██████████| 1671/1671 [30:01<00:00,  1.08s/it]
    Finishing : 100%|██████████| 5/5 [01:34<00:00, 18.84s/it]

So passing in the parquet schema is good, but not everyone will be willing/able
to generate it.