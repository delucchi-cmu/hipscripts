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

## SDSS


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