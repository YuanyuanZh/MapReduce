import zerorpc
import sys
import gevent
from gevent.queue import Queue
import job_schedule


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
        if partitions == None:
            self.partitions = {}
        self.num_mappers = num_mappers


class Master():
    def __init__(self, port, data_dir):
        self.port = port
        self.data_dir = data_dir
        self.worker_list = []
        self.worker_id = -1
        self.job_id = -1
        self.jobs = Queue()
        self.processing_jobs = {}
        self.task_id = -1
        self.task_list = []

    def getNewJobID(self):
        self.job_id += 1
        return self.job_id

    def submitJob(self, conf):
        # create job
        job = Job(conf)
        self.jobs.put_nowait(job)
        return "job submmit sucessfully"

    def registerWorker(self, worker_address):
        self.worker_id += 1
        worker = {
            "id": self.worker_id,
            "address": worker_address,
            "mapper": None,
            "reducer": None
        }
        self.worker_list.append(worker)
        return self.worker_id

    def getMapSlot(self):
        for i in range(len(self.worker_list)):
            if self.worker_list[i]['mapper'] == None:
                self.worker_list[i]['mapper'] = 'Occupied'
                return self.worker_list[i]
        return None

    def getReduceSlot(self):
        for i in range(len(self.worker_list)):
            if self.worker_list[i]['reducer'] == None:
                self.worker_list[i]['reducer'] = 'Occupied'
                return self.worker_list[i]
        return None

    def getMapResultLocation(self, job_id):
        locations = {}
        # find corresponding map result
        job = self.processing_jobs[job_id]
        map_list = job.map_task_list
        for i in range(len(map_list)):
            task = map_list[i]
            if task.state == 'Finish':
                locations[task.split_id] = task.worker
        return locations

    def assignTask(self, type, task_list):
        for i in range(len(task_list)):
            if task_list[i].state == 'NOT_ASSIGNED':
                if type == 'Map':
                    worker = self.getMapSlot()
                else:
                    worker = self.getReduceSlot()
                if worker != None:
                    task = task_list[i]
                    self.task_id += 1
                    task.task_id = self.task_id
                    task.worker = worker
                    # start task
                    client = zerorpc.Client()
                    client.connect(worker["address"])
                    if type == 'Map':
                        ret = client.startMap(task)
                    else:
                        ret = client.startReduce(task)
                    if ret == 0:
                        task.state == 'STARTING'
                    else:
                        task.state = 'NOT_ASSIGNED'

    def jobScheduler(self):
        while not self.jobs.empty():
            job = self.jobs.get()
            self.processing_jobs[job.jobId] = job
            # num_map_tasks = 0
            # num_reducer_tasks = 0
            # for i in range(len(self.worker_list)):
            #     if self.worker_list[i]['mapper'] == None:
            #         num_map_tasks += 1
            #     if self.worker_list[i]['reducer'] == None:
            #         num_reducer_tasks += 1
            partitions = []
            # assign tasks
            self.assignTask('Map', job.map_task_list)
            self.assignTask('Reduce', job.map_task_list)

    def heartBeat(self):
        return

    def run(self):
        thread1 = gevent.spawn(self.jobScheduler())
        thread2 = gevent.spawn(self.heartBeat())
        gevent.joinall([thread1, thread2])


if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = Master(port, data_dir)
    rpc_server = zerorpc.Server(master)
    addr = "tcp://0.0.0.0:" + port
    rpc_server.bind(addr)
    rpc_server.run()
    master.run()
