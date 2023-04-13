# Timedoman MVP

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

## ZTF

Make a pilot version of the catalog, using a single pixel, just to verify that we can
create the right structure.

```
mkdir /data3/epyc/data3/hipscat/catalogs/ztf_apr13
```


## PanSTARRS


## SDSS