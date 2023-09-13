k.

i'm going to try to transfer the TSP (TAPE single pixel) original catalog files to PSC. 

this is going to go great.

rsync -a --files-from=/astro/users/mmd11/git/scripts/epyc/td_mvp/tsp_files.txt /data3/epyc/data3/hipscat/catalogs/ delucchi@data.bridges2.psc.edu:/ocean/projects/phy210048p/shared/hipscat/epyc/

so what if i just try to transfer all of the zubercal files?

rsync -a --progress /data3/epyc/data3/hipscat/catalogs/zubercal/ delucchi@data.bridges2.psc.edu:/ocean/projects/phy210048p/shared/hipscat/epyc/zubercal/
