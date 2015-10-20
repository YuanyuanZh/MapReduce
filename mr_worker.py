import zerorpc
import gevent
import socket
import mr_master
import mr_classes
import input_split
import sys


class Worker():
    def __init__(self, master_address, worker_address=None):
        self.master_address = master_address
        if (worker_address == None):
            self.worker_address = self.getMyAddress()
            self.is_remote = False
        else:
            self.worker_address = worker_address
            self.is_remote = True
        self.all_map_task_list = {}
        self.all_reduce_task_list = {}

    def mapper(self, map_task):
        task = map_task
        # No this job, create it
        if self.all_map_task_list.has_key(task.job_id) == False:
            map_list = {}
            map_list[task.split_id]=task
            self.all_map_task_list[task.job_id] = map_list
        else:
            self.all_map_task_list[task.job_id][task.split_id] = task

        if task.className == 'WordCount':
            mapper = mr_classes.WordCountMap()
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
        # Map phase
        file_object = open(task.infile)
        input = self.read_input(file_object, task.splits)
        i = 0
        for j, v in enumerate(input):
            task.state = 'MAP'
            task.progress = 'Begin mapping.'
            mapper.map(j, v)
            i += 1
            percent = 100 / len(input) * i
            task.progress = "Finish " + percent + "% map task."

        # Sort intermediate keys
        table = mapper.get_table()
        keys = table.keys()
        task.state = 'SORT'
        task.progress = 'Begin sorting.'
        keys.sort()
        task.progress = 'Finish sorting.'

        # partion
        partitions = mapper.partition(keys, task.num_reducers)
        task.partitions = partitions
        task.state = 'Finish'
        task.progress = 'Finish mapping.'

    def collectAllInputsForReducer(self, task):
        #get all input from other workers
        while True:
            client1 = zerorpc.Client()
            client1.connect(self.master_address)
            #get input locations from master
            locations = client1.getMapResultLocation(task.job_id)
            for split_id,worker in locations.items():
                #if didn't has that input, get input from mapper; else ignore;
                if task.partitions.has_key(split_id) == False:
                    client = zerorpc.Client()
                    client.connect(worker['address'])
                    partition = client.getPartition(task.job_id,split_id,task.partition_id)
                    if partition != None:
                        task.partitions[split_id] = partition
                    else:
                        print "Get partition from worker %s failed" %worker['address']
            # Get all inputs
            if len(task.partitions) == task.num_mappers:
                break
            gevent.sleep(5)

    def reducer(self, reduce_task):
        task = reduce_task
        # No this job, create it
        if self.all_reduce_task_list.has_key(task.job_id) == False:
            reduce_list = {}
            reduce_list[task.partition_id]=task
            self.all_reduce_task_list[task.job_id] = reduce_list
        else:
            self.all_reduce_task_list[task.job_id][task.partition_id] = task
        if task.className == 'WordCount':
            reducer = mr_classes.WordCountReduce()
        if task.className == 'Sort':
            reducer = mr_classes.SortReduce()
        if task.className == 'hammingEnc':
            reducer = mr_classes.hammingEncReduce()
        if task.className == 'hammingDec':
            reducer = mr_classes.hammingDecReduce()
        if task.className == 'hammingFix':
            reducer = mr_classes.hammingFixReduce()
        job_for_reduces = self.collectAllInputsForReducer(task)
        reducer.set_output_oder((job_for_reduces.keys()[0])%10) # todo change (job_for_reduces.keys()[0]) to 编号
        for i in job_for_reduces.keys():
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(i,k,job_for_reduces.get(i)[k])
        reducer.write_Jason_result(self.output_base)
        reducer.write_txt_result(self.output_base)


    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"


    def getPartition(self,job_id,split_id,partition_id):
        if self.all_map_task_list.has_key(job_id):
            if self.all_map_task_list[job_id].has_key(split_id):
                return self.all_map_task_list[job_id][split_id][partition_id]
        return None


    def startRPCServer(self):
        master = zerorpc.Server(self)
        if self.is_remote:
            addr = self.worker_address
        else:
            addr = "0.0.0.0" + self.worker_address.split(":")[1]
        # addr = "tcp://0.0.0.0:"+port
        master.bind(addr)
        master.run()


    def register(self):
        self.id = self.client.registerWorker(self.worker_address)
        client = zerorpc.Client()
        client.connect(self.master_address)


    def startMap(self,task):
        thread = gevent.spawn(self.mapper(task))
        if(thread):
            return 0


    def startReduce(self, task):
        gevent.spawn(self.reducer(task))


    def run(self):
        self.register()
        self.startRPCServer()

if __name__ == '__main__':
    master_address = sys.argv[1]
    worker_address = sys.argv[2]
    worker = Worker(master_address, worker_address)
    worker.run()
