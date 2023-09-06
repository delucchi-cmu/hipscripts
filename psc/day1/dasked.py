
cluster = SLURMCluster(n_workers=0, 
job_cls: typing.Optional[dask_jobqueue.core.Job] = None, loop=None, 
security=None, shared_temp_directory=None, silence_logs='error', name=None, 
asynchronous=False, dashboard_address=None, host=None, scheduler_options=None, 
scheduler_cls=<class 'distributed.scheduler.Scheduler'>, 
interface=None, protocol=None, config_name=None, **job_kwargs)

from dask_jobqueue import SLURMCluster
cluster = SLURMCluster(
    queue='RM-shared',
    account="phy210048p",
    cores=4,
    memory="10 GB",
    interface="ib0",
    local_directory="$LOCAL",
)
cluster.scale(jobs=10) 