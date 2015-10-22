import zerorpc
import gevent
import socket

import mr_classes
import input_split
import sys
from mr_data import *
from Engines import *
import time
import json
from gevent.queue import Queue

class Worker():
    def __init__(self, master_address, worker_address=None):
        self.id = None
        self.master_address = master_address
        if (worker_address is None):
            self.worker_address = self.getMyAddress()
            self.is_remote = False
        else:
            self.worker_address = worker_address
            self.is_remote = True
        self.all_map_task_list = {}
        self.all_reduce_task_list = {}
        self.current_mapper = None
        self.current_reducer = None
        self.MapperTaskQueue = Queue()
        self.ReducerTaskQueue = Queue()

    def mapper(self, map_task):
        task = map_task
        if self.current_mapper is not None:
            if self.current_mapper.state != 'FINISH':
                print "Create mapper failed: last mapper not finished. key: %s, task_id: %s  at %s" % (
                    task.split_id, task.task_id, time.asctime(time.localtime(time.time())))
                return -1
        self.current_mapper = task
        # No this job, create it
        if self.all_map_task_list.has_key(task.job_id) == False:
            map_list = {}
            map_list[task.split_id] = task
            self.all_map_task_list[task.job_id] = map_list
        else:
            self.all_map_task_list[task.job_id][task.split_id] = task

        if task.className == 'WordCount':
            # mapper = mr_classes.WordCountMap()
            engine = WordCountEngine(task.infile, task.num_reducers, task.outfile)
        if task.className == 'Sort':
            mapper = mr_classes.SortMap()
        if task.className == 'hammingEnc':
            mapper = mr_classes.hammingEncMap()
        if task.className == 'hammingDec':
            mapper = mr_classes.hammingDecMap()
        if task.className == 'hammingFix':
            mapper = mr_classes.hammingFixMap()
        task.state = 'STARTING'
        task.progress = 'Starting.'
        partitions = engine.WordCountMapExecute(task.splits, task.split_id)

        # Map phase
        # file_object = open(task.infile)
        # input = self.read_input(file_object, task.splits)
        # i = 0
        # for j, v in enumerate(input):
        #     task.state = 'MAP'
        #     task.progress = 'Begin mapping.'
        #     mapper.map(j, v)
        #     i += 1
        #     percent = 100 / len(input) * i
        #     task.progress = "Finish " + percent + "% map task."
        #
        # # Sort intermediate keys
        # table = mapper.get_table()
        # keys = table.keys()
        # task.state = 'SORT'
        # task.progress = 'Begin sorting.'
        # keys.sort()
        # task.progress = 'Finish sorting.'
        #
        # # partion
        # partitions = mapper.partition(keys, task.num_reducers)
        task.partitions = partitions
        task.state = 'FINISH'
        task.progress = 'Finish mapping.'
        task.changeToFinish = True
        print "Finish Mapper: key: %s, task_id: %s at %s" % (
            task.split_id, task.task_id, time.asctime(time.localtime(time.time())))

    def collectAllInputsForReducer(self, task):
        # get all input from other workers
        count = 0
        while True:
            client1 = zerorpc.Client()
            client1.connect('tcp://' + self.master_address)
            # get input locations from master
            locations = client1.getMapResultLocation(task.job_id)
            for split_id, worker in locations.items():
                # if didn't has that input, get input from mapper; else ignore;
                key = str(split_id) + str(task.partition_id)
                if task.partitions.has_key(key) == False:
                    client = zerorpc.Client(timeout=300)
                    client.connect('tcp://' + worker['address'])
                    partition = client.getPartition(task.job_id, split_id, task.partition_id)
                    if partition is not None:
                        task.partitions[key] = partition
                        print "Get partition %s from worker %s successfully at %s" % (
                        key, worker['address'], time.asctime(time.localtime(time.time())))
                    else:
                        print "Get partition %s from worker %s failed at %s" % (
                        key, worker['address'], time.asctime(time.localtime(time.time())))
            # Get all inputs
            if len(task.partitions) == task.num_mappers:
                print "Get all partitions for reducer: key: %s, task_id: %s, length: %d at %s" % (
                task.partition_id, task.task_id, len(task.partitions), time.asctime(time.localtime(time.time())))
                return 0
            count+=1
            # print "reducer wait for input times: %d" %count
            gevent.sleep(5)

    def reducer(self, reduce_task):
        task = reduce_task
        if self.current_reducer is not None:
            if self.current_reducer.state != 'FINISH':
                print "Create reducer failed: last reducer not finished. key: %s, task_id: %s  at %s" % (
                    task.partition_id, task.task_id, time.asctime(time.localtime(time.time())))
                return -1
        self.current_reducer = task
        # No this job, create it
        if self.all_reduce_task_list.has_key(task.job_id) == False:
            reduce_list = {}
            reduce_list[task.partition_id] = task
            self.all_reduce_task_list[task.job_id] = reduce_list
        else:
            self.all_reduce_task_list[task.job_id][task.partition_id] = task

        if task.className == 'WordCount':
            # reducer = mr_classes.WordCountReduce()
            engine = WordCountEngine(task.infile, task.num_reducers, task.outfile)
        if task.className == 'Sort':
            reducer = mr_classes.SortReduce()
        if task.className == 'hammingEnc':
            reducer = mr_classes.hammingEncReduce()
        if task.className == 'hammingDec':
            reducer = mr_classes.hammingDecReduce()
        if task.className == 'hammingFix':
            reducer = mr_classes.hammingFixReduce()

        task.state = 'STARTING'
        task.progress = 'Starting.'

        self.collectAllInputsForReducer(task)
        # print "input partitions: ",task.partitions
        engine.WordCountReduceExecute(task.partitions, task.partition_id)
        # reducer.set_output_oder((job_for_reduces.keys()[0]) % 10)
        # for i in job_for_reduces.keys():
        #     keys = job_for_reduces[i].keys()
        #     keys.sort()
        #     for k in keys:
        #         reducer.reduce(i, k, job_for_reduces.get(i)[k])
        # reducer.write_Jason_result(self.output_base)
        # reducer.write_txt_result(self.output_base)
        task.state = 'FINISH'
        task.progress = 'Finish reducing.'
        task.changeToFinish = True
        print "Finish Reducer: key: %s, task_id: %s at %s" % (
            task.partition_id, task.task_id, time.asctime(time.localtime(time.time())))

    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"

    def getPartition(self, job_id, split_id, partition_id):
        if self.all_map_task_list.has_key(job_id):
            if self.all_map_task_list[job_id].has_key(split_id):
                key = str(split_id) + str(partition_id)
                # print "Prepare get partition %s" %(key)
                # print "Now partition keys: ", self.all_map_task_list[job_id][split_id].partitions.keys()
                return self.all_map_task_list[job_id][split_id].partitions[key]
        return None

    def getReducerResult(self, partition_id, outfile_base):
        filename = outfile_base + "_" + str(partition_id) + ".json"
        with open(filename, 'r') as f:
            data = json.load(f)
            print "Get reduce file success, partition id:", partition_id
        return data

    def startRPCServer(self):
        master = zerorpc.Server(self)
        if self.is_remote:
            addr = self.worker_address
        else:
            addr = "0.0.0.0:" + self.worker_address.split(":")[1]
        print "worker address is: %s at %s " % (addr, time.asctime(time.localtime(time.time())))
        # addr = "tcp://0.0.0.0:"+port
        master.bind('tcp://' + addr)
        master.run()

    def register(self):
        client = zerorpc.Client()
        client.connect('tcp://' + self.master_address)
        self.id = client.registerWorker(self.worker_address)

    def convertDictToMapTask(self, dict):
        task = MapTask(dict['job_id'], dict['split_id'], dict['task_id'], dict['className'], dict['worker'],
                       dict['splits'], dict['infile'], dict['partitions'], dict['num_reducers'], dict['outfile'])
        return task

    def convertDictToReduceTask(self, dict):
        task = ReduceTask(dict['job_id'], dict['partition_id'], dict['task_id'], dict['className'], dict['worker'],
                       dict['partitions'], dict['outfile'], dict['num_mappers'], dict['infile'], dict['num_reducers'])
        return task

    def startMap(self, task_dict):
        # print "Begin create map thread: at %s" % (time.asctime(time.localtime(time.time())))
        task = self.convertDictToMapTask(task_dict)
        # thread = gevent.spawn(self.mapper, task)
        # print "Create map thread: %s at %s" % (thread, time.asctime(time.localtime(time.time())))
        self.MapperTaskQueue.put(task)
        return 0

    def startReduce(self, task_dict):
        task = self.convertDictToReduceTask(task_dict)
        self.ReducerTaskQueue.put(task)
        # gevent.spawn(self.reducer(task))
        return 0

    def MapperManage(self):
        while True:
            while not self.MapperTaskQueue.empty():
                mapperTask = self.MapperTaskQueue.get()
                # print "Create map thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.mapper, mapperTask)
                print "Create map thread : key: %d at %s" % (mapperTask.split_id, time.asctime(time.localtime(time.time())))
            gevent.sleep(0)

    def ReducerManage(self):
        while True:
            while not self.ReducerTaskQueue.empty():
                reducerTask = self.ReducerTaskQueue.get()
                # print "Create reduce thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.reducer, reducerTask)
                print "Create reduce thread: key: %d at %s" % (reducerTask.partition_id, time.asctime(time.localtime(time.time())))
            gevent.sleep(0)

    def heartbeat(self):
        while True:
            if self.current_mapper is not None:
                status_mapper = MapperStatus(self.current_mapper.job_id, self.current_mapper.split_id,
                                             self.current_mapper.task_id, self.current_mapper.state,
                                             self.current_mapper.progress, self.current_mapper.changeToFinish)
                status_mapper_dict = status_mapper.__dict__
            else:
                status_mapper = None
                status_mapper_dict = None
            if self.current_reducer is not None:
                status_reducer = ReducerStatus(self.current_reducer.job_id, self.current_reducer.partition_id,
                                               self.current_reducer.task_id, self.current_reducer.state,
                                               self.current_reducer.progress, self.current_reducer.changeToFinish)
                status_reducer_dict = status_reducer.__dict__
            else:
                status_reducer = None
                status_reducer_dict = None
            status = WorkerStatus(self.id, self.worker_address, "RUNNING", status_mapper, status_reducer)
            status_dict = {
                'worker_id':self.id,
                'worker_address': self.worker_address,
                'worker_status': 'RUNNING',
                'num_heartbeat': 0,
                'num_callback':0,
                'timeout_times':0,
                'mapper_status':status_mapper_dict,
                'reducer_status':status_reducer_dict
            }
            print "send status"
            client = zerorpc.Client()
            client.connect('tcp://' + self.master_address)
            ret = client.updateWorkerStatus(status_dict)
            if ret != 0:
                print "Worker update status failed: worker_id: %s, ip: %s at %s" % (
                    self.id, self.worker_address, time.asctime(time.localtime(time.time())))
            else:
                if self.current_mapper is not None:
                    if self.current_mapper.changeToFinish == True:
                        self.current_mapper.changeToFinish = False
                if self.current_reducer is not None:
                    if self.current_reducer.changeToFinish == True:
                        self.current_reducer.changeToFinish = False
            gevent.sleep(5)


    def run(self):
        self.register()
        # self.startRPCServer()
        thread1 = gevent.spawn(self.heartbeat)
        thread2 = gevent.spawn(self.MapperManage)
        thread3 = gevent.spawn(self.ReducerManage)
        thread4 = gevent.spawn(self.startRPCServer)
        # self.startRPCServer()
        gevent.joinall([thread1,thread3,thread2,thread4])


if __name__ == '__main__':
    master_address = sys.argv[1]
    if len(sys.argv) == 3:
        worker_address = sys.argv[2]
    else:
        worker_address = None
    worker = Worker(master_address, worker_address)
    worker.run()
