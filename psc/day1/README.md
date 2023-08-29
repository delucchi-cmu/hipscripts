# PSC notes

- https://www.psc.edu/resources/bridges-2/user-guide/
- `$ ssh delucchi@bridges2.psc.edu`

## CLI

Nice utilities to understand current usage. inode restriction is important to keep in mind. Everything is going to be linked to our grant, phy210048p.

```
$ projects
$ my_quotas
```

See the jobs you have queued up:

`$ squeue -u delucchi`

And see the info for a job that already ran:

`$ job_info --slurm 18581717`

use $LOCAL for temp files?

## sbatch

Submitting batch jobs to the clusters.

sbatch options:

- `-p partition` - going to want to use "RM-shared" most of the time, I think. cheaper and likely to be scheduled faster.
- `-n n` - number of CORES requested IN TOTAL
- `--ntasks-per-node=n` - number of cores PER NODE (in RM-shared, this should be 1 to 64)
- `--mail-type=type` - (type = BEGIN, END, FAIL, or ALL). 
- `-t HH:MM:SS` - walltime requested, Walltime max == 48 hours.
- `--time-min=HH:MM:SS` minimum walltime requested. can be helpful for the scheduler.

sample batch command:

`sbatch -p RM-shared -t 5:00:00 --ntasks-per-node=32 myscript.job`

## conda

Anyway. First thing to do:

`ln -s $PROJECT/.conda ~/.conda`

^^ we have small home allocations, this puts the conda environments in the shared disk space.

Need to make some directories, so the conda create step doesn't fail.

```
interact
module load anaconda3
conda activate

// create environment
conda create -n hipscatenv python=3.10


// then back it up
conda env export >> conda_env_export.yaml

// create environment from backup
conda env create -f conda_env_export.yaml --prefix /PATH/TO/NEW_CONDA_ENV
```

## Moving data

Ok doing something. wgetting SDSS onto PSC.

```
$ screen -x sdss
```

After 1 hour, `du -chs` ==> 40G. So probably 12 hours? Check again in the morning?

Oh Beans. Maybe I should have used `data.bridges2.psc.edu`

Hmm. It's only 170G. When it was on epyc, it was 425G. That's not great?

And I did it in a screen, and didn't pipe my output anywhere, so I can't look at the logs
to see if any of the file transfers failed.

WELL. I can try that data node and do something like:

`$ source wget_sdss &> /jet/home/delucchi/git/hipscripts/psc/day1/wget_sdss.log`

Oh. Fuck me. I think data.sdss.org is down right now? Ok! 

## Misc

We don't have GPU grants, so can't do anything KBMOD-like there (without getting more resources).
we should EVENTUALLY try a run that uses multiple nodes and confirm that we're doing the right things.
