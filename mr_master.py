import zerorpc
import sys
import gevent
from gevent.queue import Queue
from mr_data import *
import random
from gevent.lock import *
import time
import input_split
import os


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
        self.collect_queue = Queue()

    def getNewJobID(self):
        self.job_id += 1
        return self.job_id

    def splitInput(self, infile,split_size,classname):
        split_hashmap = input_split.Split(infile,split_size,classname).generate_split_info()
        return split_hashmap

    def submitJob(self, conf):
        # create job
        inputFile = self.data_dir + '/' + conf['infile']
        outputFile = self.data_dir + '/' + conf['outfile']
        e = os.path.exists(inputFile)
        if e == False:
            raise IOError,"No input file"
        splits = self.splitInput(inputFile, conf['split_size'], conf['className'])
        conf['infile'] = inputFile
        conf['outfile'] = outputFile
        conf['splits'] = splits
        job = Job(conf)
        self.jobs.put_nowait(job)
        print "Initialize Job: job_id: %s at %s" % (job.jobId, time.asctime(time.localtime(time.time())))
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
        # self.worker_list[self.worker_id] = worker
        self.reportEvent('REGISTER_WORKER', worker)
        self.worker_status_list[self.worker_id] = WorkerStatus(self.worker_id, worker_address, "RUNNING", None, None)
        print "Receive register worker: worker_id: %s, ip: %s at %s" % (
            worker['id'], worker['address'], time.asctime(time.localtime(time.time())))
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
        for key, task in map_list.items():
            # task = map_list[i]
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
                    client.connect('tcp://' + worker["address"])
                    if type == 'mapper':
                        task_dict = task.__dict__
                        ret = client.startMap(task_dict)
                    else:
                        task_dict = task.__dict__
                        print "Start Reducer task"
                        ret = client.startReduce(task_dict)
                    if ret == 0:
                        task.state == 'STARTING'
                    else:
                        print "Start %s task on %s failed" % (type, worker["address"])
                        worker[type] = "Free"


    def isAllTasksFinished(self, task_list):
        count = 0
        for key in task_list.keys():
            if task_list[key].state == 'FINISH':
                count += 1
        if count == len(task_list):
            return True
        else:
            return False

    def jobScheduler(self):
        print "enter scheduler : at %s" %time.asctime( time.localtime(time.time()) )
        job = None
        while True:
            while not self.jobs.empty():
                if job is None or job.state == 'COMPLETE':
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
                    self.assignTask('reducer', job.reduce_task_list)
                    self.processing_jobs[job.jobId].state = 'PROCESSING'
                    self.processing_jobs[job.jobId].progress = '0%'
                    # while job.state != 'COMPLETE':
            while not self.event_queue.empty():
                event = self.event_queue.get_nowait()
                if event.type == 'MAPPER_FINISHED':
                    workerStatus = event.status
                    print "Receive mapper finish event: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
                        workerStatus.mapper_status.split_id, workerStatus.mapper_status.task_id, workerStatus.worker_id,
                        workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.finishMapper(event.status, job.map_task_list)
                if event.type == 'REDUCER_FINISHED':
                    workerStatus = event.status
                    print "Receive reducer finish event: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
                        workerStatus.mapper_status.split_id, workerStatus.mapper_status.task_id, workerStatus.worker_id,
                        workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.finishReducer(event.status, job.reduce_task_list)
                if event.type == 'WORKER_DOWN':
                    workerStatus = event.status
                    print "Receive worker down event: worker_id: %s, ip: %s at %s" % (
                        workerStatus.worker_id,
                        workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.processWorkerDown(event.status, job)
                if event.type == 'REGISTER_WORKER':
                    worker = event.status
                    self.worker_list[worker['id']] = worker
                    print "Register Worker: worker_id: %s, ip: %s at %s" % (
                        worker['id'], worker['address'], time.asctime(time.localtime(time.time())))
                    if job is not None:
                        self.assignTask('mapper', job.map_task_list)
                        self.assignTask('reducer', job.reduce_task_list)
                if event.type == 'COLLECT_SUCCESS':
                    print "Receive job collect success event: job_id: %s at %s" % (
                        job.jobId, time.asctime(time.localtime(time.time())))
                    # change job status
                    self.processing_jobs[job.jobId].state = 'COMPLETE'
                    self.processing_jobs[job.jobId].progress = '100%'
                    job.state = 'COMPLETE'
            # print "eee"
            gevent.sleep(0)
            # print "eee3333"

    def finishMapper(self, workerStatus, mapper_list):
        # update task status
        task = mapper_list[workerStatus.mapper_status.split_id]
        task.state = workerStatus.mapper_status.state
        task.progress = workerStatus.mapper_status.progress
        # update worker assign state
        self.worker_list[workerStatus.worker_id]['mapper'] = 'Free'
        # assgin new mapper task to this slot
        self.assignTask('mapper', mapper_list)
        print "Finish Mapper: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
            task.split_id, task.task_id, task.worker['id'], task.worker['address'],
            time.asctime(time.localtime(time.time())))

    def finishReducer(self, workerStatus, reducer_list):
        # update task status
        task = reducer_list[workerStatus.reducer_status.partition_id]
        task.state = workerStatus.reducer_status.state
        task.progress = workerStatus.reducer_status.progress
        # update worker assign state
        self.worker_list[workerStatus.worker_id]['reducer'] = 'Free'
        # assgin new reducer task to this slot
        self.assignTask('reducer', reducer_list)
        # check if all reducers finished
        if self.isAllTasksFinished(reducer_list):
            self.collect_queue.put(reducer_list)
            # self.reportEvent('NEED_COLLECT', reducer_list)
            # ret = self.collectResults(reducer_list)
            # if ret == 0:
            #     #change job status
            #     self.processing_jobs[task.job_id].state = 'COMPLETE'
            #     self.processing_jobs[task.job_id].progress = '100%'
        print "Finish Reducer: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
            task.partition_id, task.task_id, task.worker['id'], task.worker['address'],
            time.asctime(time.localtime(time.time())))
        return

    def collectJobResult(self):
        print "enter collect : at %s" %time.asctime( time.localtime(time.time()) )
        while True:
            while not self.collect_queue.empty():
                reducer_list = self.collect_queue.get_nowait()
                ret = self.collectResults(reducer_list)
                if ret == 0:
                    self.reportEvent('COLLECT_SUCCESS', None)
                else:
                    print "Collect result failed at: ", time.asctime(time.localtime(time.time()))
            gevent.sleep(0)

    def mergeData(self, data_list, output_filename):
        data_dict = {}
        for k in data_list:
            keys = k.keys()
            for key in keys:
                data_dict[key] = k.get(key)

        keys = data_dict.keys()
        keys.sort()
        out_str = ''
        for key in keys:
            s = ''
            vaule = data_dict.get(key)
            for v in vaule:
                s += v
            out_str += s
        out_file = open(output_filename, 'w')
        out_file.write(str(out_str))
        return

    def collectResults(self, reducer_list):
        data_list = []
        for key, task in reducer_list.items():
            client = zerorpc.Client()
            client.connect('tcp://' + task.worker['address'])
            data = client.getReducerResult(task.partition_id, task.outfile)
            if data is not None:
                data_list.append(data)
            else:
                print "Get reduce result failed on worker: id: %s, address:%s %s" % (
                    task.worker['id'], task.worker['address'], time.asctime(time.localtime(time.time())))
                return -1
        self.mergeData(data_list, task.outfile)
        return 0

    def processWorkerDown(self, workerStatus, job):
        print "Processing worker down: key: worker_id: %s, ip: %s at %s" % (
            workerStatus.worker_id, workerStatus.worker_address, time.asctime(time.localtime(time.time())))
        # remove worker from worker list
        del self.worker_list[workerStatus.worker_id]
        del self.worker_status_list[workerStatus.worker_id]
        # if reducer down, clean this reducer task
        if workerStatus.reducer_status is not None:
            task = job.reduce_task_list[workerStatus.reducer_status.partition_id]
            task.task_id = None
            task.worker = None
            task.state = 'NOT_ASSIGNED'
            task.progress = '0'
        if job is not None:
            self.assignTask("reducer", job.reduce_task_list)

        # if mapper down, clean this mapper task
        if workerStatus.mapper_status is not None:
            task = job.map_task_list[workerStatus.mapper_status.split_id]
            task.task_id = None
            task.worker = None
            task.state = 'NOT_ASSIGNED'
            task.progress = '0'
        if job is not None:
            self.assignTask('mapper', job.map_task_list)

    def heartBeat(self):
        print "enter heartbeat : at %s" %time.asctime( time.localtime(time.time()) )
        while True:
            for worker_id in self.worker_status_list.keys():
                #
                workerStatus = self.worker_status_list[worker_id]
                if workerStatus.num_heartbeat == workerStatus.num_callback:
                    workerStatus.timeout_times += 1
                    if workerStatus.timeout_times == 3:
                        workerStatus.worker_status = 'DOWN'
                        self.reportEvent('WORKER_DOWN', workerStatus)
                        print "Find worker down: worker_id: %s, ip: %s at %s" % (
                            workerStatus.worker_id, workerStatus.worker_address,
                            time.asctime(time.localtime(time.time())))
                else:
                    workerStatus.num_heartbeat = workerStatus.num_callback
                    workerStatus.timeout_times = 0
            gevent.sleep(5)

    def reportEvent(self, type, status):
        event = Event(type, status)
        self.event_queue.put_nowait(event)

    def convertDictToWorkerStatus(self, dict):
        mapper_status_dict = dict['mapper_status']
        if mapper_status_dict is not None:
            mapper_status = MapperStatus(mapper_status_dict['job_id'], mapper_status_dict['split_id'],mapper_status_dict['task_id'],mapper_status_dict['state'],mapper_status_dict['progress'],mapper_status_dict['changeToFinish'])
        else:
            mapper_status = None
        reducer_status_dict = dict['reducer_status']
        if reducer_status_dict is not None:
            reducer_status = ReducerStatus(reducer_status_dict['job_id'],reducer_status_dict['partition_id'],reducer_status_dict['task_id'],reducer_status_dict['state'],reducer_status_dict['progress'],reducer_status_dict['changeToFinish'])
        else:
            reducer_status = None
        worker_status = WorkerStatus(dict['worker_id'],dict['worker_address'],dict['worker_status'],mapper_status,reducer_status)
        return worker_status

    def updateWorkerStatus(self, workerStatus_dict):
        workerStatus = self.convertDictToWorkerStatus(workerStatus_dict)
        # print " call back"
        # check status
        origin_status = self.worker_status_list[workerStatus.worker_id]
        if origin_status.worker_status != 'Down':
            if workerStatus.mapper_status is not None:
                # check task state
                if workerStatus.mapper_status.changeToFinish == True:
                    self.reportEvent('MAPPER_FINISHED', workerStatus)
            if workerStatus.reducer_status is not None:
                if workerStatus.reducer_status.changeToFinish == True:
                    self.reportEvent('REDUCER_FINISHED', workerStatus)
        self.worker_status_list[workerStatus.worker_id].mapper_status = workerStatus.mapper_status
        self.worker_status_list[workerStatus.worker_id].reducer_status = workerStatus.reducer_status
        self.worker_status_list[workerStatus.worker_id].num_callback = random.random()
        return 0

    def run(self):
        thread1 = gevent.spawn(self.jobScheduler)
        print "Create job scheduler thread : %s at %s" %(thread1,time.asctime( time.localtime(time.time()) ))
        thread2 = gevent.spawn(self.heartBeat)
        print "Create heartbeat thread : %s at %s" %(thread2,time.asctime( time.localtime(time.time()) ))
        thread3 = gevent.spawn(self.collectJobResult)
        print "Create job collect thread : %s at %s" %(thread3,time.asctime( time.localtime(time.time()) ))
        thread4 = gevent.spawn(self.rpcServer)
        gevent.joinall([thread1,thread2,thread3,thread4])
        # gevent.joinall([gevent.spawn(self.jobScheduler()), gevent.spawn(self.heartBeat()), gevent.spawn(self.collectJobResult())])

    def rpcServer(self):
        print "enter rpc"
        rpc_server = zerorpc.Server(self)
        addr = "tcp://0.0.0.0:" + self.port
        print "address: %s", addr
        rpc_server.bind(addr)
        print "rpc run 1"
        rpc_server.run()
        print "rpc run"

if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = Master(port, data_dir)
    master.run()
    # rpc_server = zerorpc.Server(master)
    # addr = "tcp://0.0.0.0:" + port
    # rpc_server.bind(addr)
    # print "rpc run 1"
    # rpc_server.run()
    # print "rpc run"


