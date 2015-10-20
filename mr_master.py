import zerorpc
import sys
import gevent
from gevent.queue import Queue
from mr_data import *
import random
from gevent.lock import *


class Master():
    def __init__(self, port, data_dir):
        self.port = port
        self.data_dir = data_dir
        self.worker_list = {}
        self.worker_id = -1
        self.job_id = -1
        self.jobs = Queue()
        self.processing_jobs = {}
        self.task_id = -1
        self.task_list = []
        self.worker_status_list = {}
        self.event_queue = Queue()

    def getNewJobID(self):
        self.job_id += 1
        return self.job_id

    def submitJob(self, conf):
        # create job
        job = Job(conf)
        self.jobs.put_nowait(job)
        return 0

    def getJobStatus(self, job_id):
        job = self.processing_jobs[job_id]
        return job.status, job.progress


    def registerWorker(self, worker_address):
        self.worker_id += 1
        worker = {
            "id": self.worker_id,
            "address": worker_address,
            "mapper": 'Free',
            "reducer": 'Free'
        }
        #self.worker_list[self.worker_id] = worker
        self.reportEvent('REGISTER_WORKER', worker)
        self.worker_status_list[self.worker_id] = WorkerStatus(self.worker_id, worker_address, "RUNNING", None, None)
        return self.worker_id

    def getMapSlot(self):
        for worker_id in self.worker_list.keys():
            if self.worker_list[worker_id]['mapper'] == 'Free':
                self.worker_list[worker_id]['mapper'] = 'Occupied'
                return self.worker_list[worker_id]
        return None

    def getReduceSlot(self):
        for i in range(len(self.worker_list)):
            if self.worker_list[i]['reducer'] == 'Free':
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
            if task.state == 'FINISH':
                locations[task.split_id] = task.worker
        return locations

    def assignTask(self, type, task_list):
        for i in range(len(task_list)):
            if task_list[i].state == 'NOT_ASSIGNED':
                if type == 'mapper':
                    worker = self.getMapSlot()
                else:
                    worker = self.getReduceSlot()
                if worker is not None:
                    task = task_list[i]
                    self.task_id += 1
                    task.task_id = self.task_id
                    task.worker = worker
                    # start task
                    client = zerorpc.Client()
                    client.connect(worker["address"])
                    if type == 'mapper':
                        ret = client.startMap(task)
                    else:
                        ret = client.startReduce(task)
                    if ret == 0:
                        task.state == 'STARTING'
                    else:
                        print "Start %s task on %s failed" % (type, worker["address"])
                        worker[type] = None

    def isAllTasksFinished(self,task_list):
        count = 0
        for key in task_list.keys():
            if task_list[key].state == 'FINISH':
                count += 1
        if count == len(task_list):
            return True
        else:
            return False

    def jobScheduler(self):
        while True:
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

                # assign tasks in creating job
                self.assignTask('mapper', job.map_task_list)
                self.assignTask('reducer', job.map_task_list)
                self.processing_jobs[job.jobId].state = 'PROCESSING'
                self.processing_jobs[job.jobId].progress = '0%'
                while job.state != 'COMPLETE':
                    while not self.event_queue.empty():
                        event = self.event_queue.get_nowait()
                        if event.type == 'MAPPER_FINISHED':
                            self.finishMapper(event.status, job.map_task_list)
                        if event.type == 'REDUCER_FINISHED':
                            self.finishReducer(event.status, job.reduce_task_list)
                        if event.type == 'WORKER_DOWN':
                            self.processWorkerDown(event.status, job)
                        if event.type == 'REGISTER_WORKER':
                            worker = event.status
                            self.worker_list[worker['id']] = worker
                            self.assignTask('mapper', job.map_task_list)
                            self.assignTask('reducer', job.map_task_list)
                        # if event.type == 'NEED_COLLECT':
                        #     #finish map and reduce, let main thread to collect results
                        #     break



    def finishMapper(self, workerStatus, mapper_list):
        #update task status
        task = mapper_list[workerStatus.mapper_status.split_id]
        task.state = workerStatus.mapper_status.state
        task.progress = workerStatus.mapper_status.progress
        #update worker assign state
        self.worker_list[workerStatus.worker_id]['mapper'] = 'Free'
        #assgin new mapper task to this slot
        self.assignTask('mapper', mapper_list)

    def finishReducer(self, workerStatus, reducer_list):
        #update task status
        task = reducer_list[workerStatus.reducer_status.split_id]
        task.state = workerStatus.reducer_status.state
        task.progress = workerStatus.reducer_status.progress
        #update worker assign state
        self.worker_list[workerStatus.worker_id]['reducer'] = 'Free'
        #assgin new reducer task to this slot
        self.assignTask('reducer', reducer_list)
        #check if all reducers finished
        if self.isAllTasksFinished(reducer_list):
            # self.reportEvent('NEED_COLLECT', reducer_list)
            ret = self.collectResults(reducer_list)
            if ret == 0:
                #change job status
                self.processing_jobs[task.job_id].state = 'COMPLETE'
                self.processing_jobs[task.job_id].progress = '100%'
        return

    def mergeData(self, data_list, filename):
        return

    def collectResults(self, reducer_list):
        data_list = []
        for key, task in reducer_list.items():
            client = zerorpc.Client()
            client.connect(task.worker['address'])
            data = client.getReducerResult(task.partition_id,task.outfile)
            if data is not None:
                data_list.append(data)
            else:
                return -1
        self.mergeData(data_list,task.outfile)
        return 0

    def processWorkerDown(self,workerStatus, job):
        #remove worker from worker list
        del self.worker_list[workerStatus.worker_id]
        del self.worker_status_list[workerStatus.worker_id]
        #if reducer down, clean this reducer task
        if workerStatus.reducer_status is not None:
            task = job.reduce_task_list[workerStatus.reducer_status.partition_id]
            task.task_id = None
            task.worker = None
            task.state = 'NOT_ASSIGNED'
            task.progress = '0'
        self.assignTask("mapper", job.reduce_task_list)
        #if mapper down, clean this mapper task
        if workerStatus.mapper_status is not None:
            task = job.map_task_list[workerStatus.mapper_status.split_id]
            task.task_id = None
            task.worker = None
            task.state = 'NOT_ASSIGNED'
            task.progress = '0'
        self.assignTask('reducer',job.map_task_list)

    def heartBeat(self):
        while True:
            for worker_id in self.worker_status_list.keys():
                #
                workerStatus = self.worker_status_list[worker_id]
                if workerStatus.num_heartbeat == workerStatus.num_callback:
                    workerStatus.timeout_times += 1
                    if workerStatus.timeout_times == 3:
                        workerStatus.worker_status = 'DOWN'
                        self.reportEvent('WORKER_DOWN', workerStatus)
                else:
                    workerStatus.num_heartbeat = workerStatus.num_callback
                    workerStatus.timeout_times = 0
            gevent.sleep(5)

    def reportEvent(self, type, status):
        event = Event(type, status)
        self.event_queue.put_nowait(event)

    def updateWorkerStatus(self, workerStatus):
        # check status
        orgin_status = self.worker_status_list[workerStatus.id]
        if orgin_status.worker_status != 'Down':
            if workerStatus.mapper_status is not None:
                # check task state
                if workerStatus.mapper_status.stateChange == True:
                    self.reportEvent('MAPPER_FINISHED', workerStatus)
            if workerStatus.mapper_status is not None:
                if workerStatus.reducer_status.stateChange == True:
                    self.reportEvent('REDUCER_FINISHED', workerStatus)
        self.worker_status_list[workerStatus.id].mapper_status = workerStatus.mapper_status
        self.worker_status_list[workerStatus.id].reducer_status = workerStatus.reducer_status
        self.worker_status_list[workerStatus.id].num_callback = random.random()
        return 0

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
