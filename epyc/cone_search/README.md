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
