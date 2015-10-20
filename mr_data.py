class Event():
    def __init__(self,type,status):
        self.type = type
        self.status = status

class MapperStatus():
    def __init__(self,job_id,split_id,task_id,state, progress,stateChange):
        self.job_id = job_id
        self.split_id = split_id
        self.task_id = task_id
        self.state = state
        self.progress = progress
        self.stateChange = stateChange

class ReducerStatus():
    def __init__(self,job_id,partition_id,task_id,state, progress,stateChange):
        self.job_id = job_id
        self.partition_id = partition_id
        self.task_id = task_id
        self.state = state
        self.progress = progress
        self.stateChange = stateChange

class WorkerStatus():
    def __init__(self, worker_id, worker_address,worker_status,mapper_status,reducer_status):
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
                           self.num_reducers)
            self.map_task_list[i] = task
        self.reduce_task_list = {}
        for i in range(self.num_reducers):
            task = ReduceTask(self.jobId, i, None, self.className, None, None, self.outfile, len(self.splits))
            self.reduce_task_list[i] = task

    def setStatus(self, status):
        self.status = status

    def setProgress(self, progress):
        self.progress = progress


class Task():
    def __init__(self, job_id, task_id, className, worker):
        self.task_id = task_id
        self.job_id = job_id
        self.className = className
        self.worker = worker
        self.state = "NOT_ASSIGNED"
        self.progress = "0"
        self.stateChange = False


class MapTask(Task):
    def __init__(self, job_id, split_id, task_id, className, worker, splits, infile, partitions, num_reducers):
        Task.__init__(self, job_id, task_id, className, worker)
        self.split_id = split_id
        self.splits = splits
        self.infile = infile
        self.partitions = partitions
        self.num_reducers = num_reducers


class ReduceTask(Task):
    def __init__(self, job_id, partition_id, task_id, className, worker, partitions, outfile, num_mappers):
        Task.__init__(self, job_id, task_id, className, worker)
        self.partition_id = partition_id
        self.outfile = outfile
        if partitions is not None:
            self.partitions = {}
        self.num_mappers = num_mappers
