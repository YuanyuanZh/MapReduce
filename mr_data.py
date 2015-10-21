class Event():
    def __init__(self, type, status):
        self.type = type
        self.status = status


class MapperStatus():
    def __init__(self, job_id, split_id, task_id, state, progress, changeToFinish):
        self.job_id = job_id
        self.split_id = split_id
        self.task_id = task_id
        self.state = state
        self.progress = progress
        self.changeToFinish = changeToFinish


class ReducerStatus():
    def __init__(self, job_id, partition_id, task_id, state, progress, changeToFinish):
        self.job_id = job_id
        self.partition_id = partition_id
        self.task_id = task_id
        self.state = state
        self.progress = progress
        self.changeToFinish = changeToFinish


class WorkerStatus():
    def __init__(self, worker_id, worker_address, worker_status, mapper_status, reducer_status):
        self.worker_id = worker_id
        self.worker_address = worker_address
        self.worker_status = worker_status
        self.num_heartbeat = 0
        self.num_callback = 0
        self.timeout_times = 0
        self.mapper_status = mapper_status
        self.reducer_status = reducer_status


class Job():
    def __init__(self, conf):
        self.jobId = conf['jobId']
        self.splits = conf['splits']
        self.className = conf['className']
        self.num_reducers = conf['num_reducers']
        self.infile = conf['infile']
        self.outfile = conf['outfile']
        self.status = "INITIALIZING"
        self.progress = "0%"
        # create empty task list
        self.map_task_list = {}
        for i in range(len(self.splits)):
            task = MapTask(self.jobId, i, None, self.className, None, self.splits[i], self.infile, None,
                           self.num_reducers, self.outfile)
            self.map_task_list[i] = task
        self.reduce_task_list = {}
        for i in range(self.num_reducers):
            task = ReduceTask(self.jobId, i, None, self.className, None, None, self.outfile, len(self.splits),
                              self.infile, self.num_reducers)
            self.reduce_task_list[i] = task

    def setStatus(self, status):
        self.status = status

    def setProgress(self, progress):
        self.progress = progress


class Task():
    def __init__(self, job_id, task_id, className, worker, infile, outfile, num_reducers):
        self.task_id = task_id
        self.job_id = job_id
        self.className = className
        self.worker = worker
        self.state = "NOT_ASSIGNED"
        self.progress = "0"
        self.changeToFinish = False
        self.infile = infile
        self.outfile = outfile
        self.num_reducers = num_reducers


class MapTask(Task):
    def __init__(self, job_id, split_id, task_id, className, worker, splits, infile, partitions, num_reducers, outfile):
        Task.__init__(self, job_id, task_id, className, worker, infile, outfile, num_reducers)
        self.split_id = split_id
        self.splits = splits
        self.partitions = partitions


class ReduceTask(Task):
    def __init__(self, job_id, partition_id, task_id, className, worker, partitions, outfile, num_mappers, infile,
                 num_reducers):
        Task.__init__(self, job_id, task_id, className, worker, infile, outfile, num_reducers)
        self.partition_id = partition_id
        if partitions is not None:
            self.partitions = {}
        self.num_mappers = num_mappers
