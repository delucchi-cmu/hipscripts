# Timedomain MVP

See doc

https://docs.google.com/document/d/1n9bJM9f3XkqmuNofuBECBJXwj2h86grDjxMrbW_X-do/edit

In the below notes, I'll use $TD_MVP_DIR to refer to this directory.

## AGN source

```
cd /data3/epyc/data3/hipscat/raw/
mkdir dr16q
cd dr16q
wget http://quasar.astro.illinois.edu/paper_data/DR16Q/dr16q_prop_Oct23_2022.fits.gz
```

```
cd $TD_MVP_DIR
python agn.py
```

The "constant" pipeline ran with a single worker, and had runtime like:

```
real    4m57.939s
user    5m28.340s
sys     0m56.030s
```

## ZTF

Make a pilot version of the catalog, using a single pixel, just to verify that we can
create the right structure.

```
mkdir /data3/epyc/data3/hipscat/catalogs/ztf_apr13
```

and this has all been a huge pain in my butt.

## PanSTARRS

### detection

    real    5351m9.433s
    user    34341m30.827s
    sys     6731m22.640s

## zubercal

### bands

zubercal files are split into parquet files by ra/dec/band. 

the band is included in the file name, so we can use a regular expression to extract the 
band, and add it as a column to our data when we read it in. no big whoop - and the way 
we've structured the filereader stuff, it's pretty easy to sneak it in.

### duplicate index

the pre-existing pandas index contains duplicates for some files. this is a little frustrating.

this breaks down at the map_reduce line:

```
filtered_data = data.filter(items=data_indexes, axis=0)
```

With a ValueError - cannot reindex on an axis with duplicate labels

The quickest way I could think to work around this is to ignore the pandas index column when
we read in the parquet files. so just use the 10 columns we DO want.

    mjd
    mag
    objdec
    objra
    magerr
    objectid
    info
    flag
    rcidin
    fieldid

this seems to work. and i'm not sure what the best long-term strategy is here. i want to look
more into the duplicated indexes and see if it's really a problem.

**after some digging**

they're using the panstarrs id (`objectid` column) as the index.

- this makes it non-unique
- they're already storing this value in the `objectid` column, and parquet isn't compressing
the duplication away
- this makes me sad. but i think that just ignoring it for this use case and importing all 
the OTHER fields is ok.

### some parquet problems

`/epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/F0065/ztf_0065_1990_g.parquet`

    Exception: 'OSError("Couldn't deserialize thrift: don't know what type: 
    Deserializing page header failed.")'

waiting for more.

there are > 500_000 parquet files. this makes the dask / futures model break. hard. it
just can't schedule and keep up with the number of tasks needed. the whole pipeline
failed with a CancelledError after 89 minutes, and no mapping stages completed.

options:

- try on ray (the cop-out, that i'm trying anyway).
- try chunking the input file list, and only hand off ~1000 files at once.
- try chunking the input file list, and send a whole chunk to a task.

### some ray problems

**temp file length**

OSError: AF_UNIX path length cannot exceed 107 bytes

    /data3/epyc/projects3/mmd11_hipscat/ray_spill/session_2023-06-16_09-22-19_341436_129856/sockets/plasma_store

Trying instead with a spill dir like:

    /data3/epyc/projects3/mmd11/ray/

**warnings**

    UserWarning: Configuration key "shuffle" has been deprecated. Please use "dataframe.shuffle.algorithm" instead

This is not coming from user code anywhere. Must be within dask.

**also fails**

It failed after 61 minutes. no mapping stages completed.

### with planning refactor

1. first attempt:

    Mapping  :   0%|          | 15/521429 [00:18<61:45:51,  2.34it/s]

there weren't really enough workers to have started this job with 30 workers, and 
the worker creation **struggled**.

2. second attempt:

    Mapping  :   0%|          | 0/521414 [00:00<?, ?it/s]

worker creation / execution / waiting again **struggled**. And it takes three+
keyboard interrupts to actually interrupt this S*&^.

but this is also in a totally new conda environment. i'm going to check that the
pipeline can run on a smaller batch first...

3. third attempt:

recreated the conda environment with python 3.10.

took a few minutes, but we're here:

    Mapping  :   0%|          | 0/521414 [00:00<?, ?it/s]
    Mapping  :   0%|          | 1/521414 [00:58<8543:16:45, 58.99s/it]

aaaannnnnddd i've been waiting like 10 minutes for another iteration to finish.
the reduce stages don't have this kind of problem, and looking at the timeouts, 
they're happening in a result callback. going to try refactoring the callback
out. wish me luck.

4. fourth attempt

callback refactor.

    Mapping  :   0%|          | 0/521429 [00:00<?, ?it/s]
    Mapping  :   0%|          | 3/521429 [01:27<4212:30:05, 29.08s/it]
    Mapping  :   0%|          | 16/521429 [01:27<586:57:44,  4.05s/it]
    Mapping  :   0%|          | 16/521429 [01:42<586:57:44,  4.05s/it]

aaaaaaaaand hung. it's still taking a long time. could be that there's still a
return value from the map call?

also. just let me kill this stupid job, dask. let me kill it.

5. fifth attempt

    Mapping  :   0%|          | 0/521413 [00:00<?, ?it/s]

but we've been here before. i need to just walk away for an hour.

6. clear out the temp directory?

    Mapping  :   0%|          | 0/521429 [00:00<?, ?it/s]
    Mapping  :   0%|          | 260/521429 [01:21<2:13:30, 65.06it/s]
    Mapping  :   0%|          | 260/521429 [01:36<2:13:30, 65.06it/s]

well this is going better. but i don't love that it gave me two entries for 
260, and then froze after that.

showing two entries would normally mean that the tqdm state is now closed.
which unfortunately might be true?

16 hours later, and nothing.

7. go by directory. not the full on file path generator thing i'd written up,
but way simpler, and just requires REMOVING code. i'm a fan =]

    Mapping  :   0%|          | 0/721 [00:00<?, ?it/s]
    Mapping  :   6%|▋         | 46/721 [50:58<5:38:17, 30.07s/it]2023-06-28 13:20:37,272 - distributed.worker - WARNING - Compute Failed
Key:       map_13
Function:  map_to_pixels
args:      ()
kwargs:    {'input_file': '/epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/F0065/', 'cache_path': '/data3/epyc/data3/hipscat/tmp/zubercal/zubercal/intermediate', 'file_reader': <__main__.ZubercalParquetReader object at 0x7f9a4c612d40>, 'mapping_key': 'map_13', 'highest_order': 10, 'ra_column': 'objra', 'dec_column': 'objdec'}
Exception: 'OSError("Couldn\'t deserialize thrift: don\'t know what type: \\x0f\\nDeserializing page header failed.\\n")'

    Mapping  : 100%|██████████| 721/721 [14:32:57<00:00, 72.65s/it] 

so. pretty good. one of the files failed, and i need to dig into that, but
otherwise, only takes around 15 hours to read through all the files to 
get the histogram.

    real    873m21.594s
    user    2872m16.485s
    sys     1043m44.676s

7.1. found the troublesome file:

    /epyc/data/ztf_matchfiles/zubercal_dr16/atua.caltech.edu/F0065/ztf_0065_1990_g.parquet

the other 742 in that directory did ok.

7.2. resuming.

    Mapping  :   0%|          | 0/1 [00:00<?, ?it/s]
    Mapping  : 100%|██████████| 1/1 [02:37<00:00, 157.99s/it]

    Binning  :   0%|          | 0/1 [00:00<?, ?it/s]
    Splitting: 100%|██████████| 721/721 [243:53:48<00:00, 1217.79s/it]
    Reducing :  69%|██████▉   | 49077/70853 [48:46:28<18:16:56,  3.02s/it]
    Reducing : 100%|██████████| 70853/70853 [70:41:20<00:00,  3.59s/it]

    Finishing:   0%|          | 0/6 [00:00<?, ?it/s]
    Finishing: 100%|██████████| 6/6 [19:52<00:00, 198.80s/it]

with timing:

    real    18908m8.470s
    user    55157m1.188s
    sys     13705m41.481s
    
Y'all. Y'ALL. 

So ready to just walk away from this. Maybe I'll go get some ice cream.

## SDSS

Using data from https://data.sdss.org/sas/dr12/boss/sweeps/dr9/301/

See also https://live-sdss4org-dr16.pantheonsite.io/imaging/catalogs/#sweeps

Multidimensional FITS files. harumph.

raw file size

    $ du -> 425G

1. Remove all the multidimensional fields on import. 

This is a terrible option, as all the useful information is in those
multidimensional fields.

    real    52m52.021s
    user    458m22.770s
    sys     84m49.995s

    $ du  -> 30G

2. Convert all the fits files to parquet, with FIELD_r, FIELD_i, etc expansion

Time to convert all files: 4:45:40

Time to import:

    real    37m27.010s
    user    293m57.470s
    sys     91m48.088s

    Mapping  : 100%|██████████| 4584/4584 [03:18<00:00, 23.04it/s]
    Binning  : 100%|██████████| 2/2 [00:09<00:00,  4.67s/it]
    Splitting: 100%|██████████| 4584/4584 [17:16<00:00,  4.42it/s]
    Reducing : 100%|██████████| 263/263 [15:36<00:00,  3.56s/it]
    Finishing: 100%|██████████| 6/6 [00:01<00:00,  3.35it/s]

    $ du -> 209G

3. Convert to nested array parquet files?

Yikes. That's 7 minutes per file to convert. That would take over a week.

These byte-swapped arrays are killing me.