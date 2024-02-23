## What's a nice cone search?

Looking at gaia_symbolic, I think we've got a nice chunk here:


/epyc/projects3/sam_hipscat/gaia/catalog/

    ra = 2  # degrees
    dec = -80  # degrees
    radius = 2  # degrees

num partitions in cone: 4
all partitions [Order: 3, Pixel: 514, Order: 5, Pixel: 8320, Order: 3, Pixel: 705, Order: 3, Pixel: 708]


So I need to copy the following files up to the abfs cloud partition:

/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/catalog_info.json
/data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/_metadata
/epyc/projects3/sam_hipscat/gaia/catalog/Norder=3/Dir=0/Npix=514.parquet
...

## Re-importing gaia on EPYC

wget --content-disposition --trust-server-names -i /astro/users/mmd11/git/scripts/epyc/cone_search/gaia_files.txt

wget --content-disposition --trust-server-names -i /astro/users/mmd11/git/scripts/epyc/cone_search/gaia_files2.txt

10 workers, 50 files

    Mapping   : 100%|██████████| 50/50 [01:20<00:00,  1.60s/it]
    Binning   : 100%|██████████| 2/2 [00:06<00:00,  3.45s/it]
    Splitting : 100%|██████████| 50/50 [02:05<00:00,  2.52s/it]
    Reducing  : 100%|██████████| 60/60 [00:48<00:00,  1.24it/s]
    Finishing : 100%|██████████| 5/5 [00:00<00:00, 11.19it/s]

So if we try it again, with 3386 files, and ... 50 workers?

    real    131m49.368s
    user    2237m22.010s
    sys     502m11.815s

