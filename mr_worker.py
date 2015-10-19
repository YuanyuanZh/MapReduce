import zerorpc
import gevent
import socket
import mr_master
import mr_classes
import input_split


class Worker():
    def __init__(self, master_address, worker_address=None):
        self.master_address = master_address
        if (worker_address == None):
            self.worker_address = self.getMyAddress()
            self.is_remote = False
        else:
            self.worker_address = worker_address
            self.is_remote = True
        self.map_task_list = []
        self.reduce_task_list = []

    def mapper(self, map_task):
        task = map_task
        self.map_task_list.append(task)
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
        file_object = open(task.input)
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
        task.output = partitions
        task.progress = 'Finish mapping.'

    def reducer(self, reduce_task):
        task = reduce_task
        while True:
            for (k,v) in task.splits:
                client = zerorpc.Client()
                k

        for i in range(len(job_for_reduces)):
            reducer = self.create_reduce_instance(self.class_name)
            keys = job_for_reduces[i].keys()
            keys.sort()
            for k in keys:
                reducer.reduce(k, job_for_reduces[i][k])
            reducer.write_result(self.output_base,i)
        print('Explicit context to reducer')
        gevent.sleep(0)
        print('Implicit context switch back to reducer')


    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"


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


    def startMap(self, task):
        gevent.spawn(self.mapper(task))


    def startReduce(self, task):
        gevent.spawn(self.reducer(task))


    def run(self):
        self.register()
        self.startRPCServer()
        gevent.joinall([
            gevent.spawn(self.mapper),
            gevent.spawn(self.reducer),
        ])
