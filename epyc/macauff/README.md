## Dec 8

Lessee if I can find the input data.

### gaia catalog
* try: /epyc/projects3/sam_hipscat/gaia/catalog/

looks promising!
but the _metadata file is not good. and I can't overwrite it. 
so i'm going to make a symbolic link to the parquet files and make a "new" catalog

ln -s [target] [symlink]

ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=2 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=2
ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=3 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=3
ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=4 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=4
ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=5 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=5
ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=6 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=6
ln -s /epyc/projects3/sam_hipscat/gaia/catalog/Norder=7 /data3/epyc/data3/hipscat/test_catalogs/gaia_symbolic/Norder=7



### catwise catalog

* try: /epyc/projects3/sam_hipscat/catwise2020/catwise2020/
    looks like a catalog!

### raw assocation CSVs

* try: /data3/epyc/projects3/sean_hipscat/
* try: /data3/epyc/hipscat/raw/
* found something:
    * /data3/epyc/data3/hipscat/raw/macauff_results/rds/project/iris_vol3/rds-iris-ip005/tjw/dr3_catwise_allskytest/output_csvs/
    * with ~1,500 subdirectories...
    * try 2309.

### metadata yml file

I think this is just in the shared drive. look there.