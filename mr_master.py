import zerorpc
import sys
import gevent
from gevent.queue import Queue


class Job():
    def __init__(self, conf):
        self.jobId = conf['jobId']
        self.splits = conf['splits']
        self.num_reducers = conf['num_reducers']
        self.infile = conf['infile']
        self.outfile = conf['outfile']
        self.status = "INITIALIZING"
        self.progress = "0%"
        self.map_task_list = []
        self.reduce_task_list = []

    def setStatus(self, status):
        self.status = status

    def setProgress(self, progress):
        self.progress = progress

class Task():
    def __init__(self,task_id,type,worker,splits,input,output):
        self.task_id = task_id
        self.type = type
        self.worker = worker
        self.splits = splits
        self.input = input
        self.output = output
        self.state = "STARTING"
        self.progress = "0"

class Master():
    def __init__(self, port,data_dir):
        self.port = port
        self.data_dir = data_dir
        self.worker_list = []
        self.worker_id = -1
        self.job_id = -1
        self.jobs = Queue()
        self.task_id = -1
        self.task_list=[]

    def getNewJobID(self):
        self.job_id += 1
        return self.job_id

    def submitJob(self, conf):
        #create job
        job = Job(conf)
        self.jobs.put_nowait(job)
        return "job submmit sucessfully"

    def registerWorker(self, worker_address):
        self.worker_id += 1
        worker = {
            "id":self.worker_id,
            "address": worker_address
        }
        self.worker_list.append(worker)
        return self.worker_id

    def jobScheduler(self):
        while not self.jobs.empty():
            job = self.jobs.get()
            #create map tasks
            num_map_tasks = len(self.worker_list)
            map_splits = getMapSplits(job.splits,num_map_tasks)
            partitions = []
            for i in range(num_map_tasks):
                self.task_id += 1
                task = Task(self.task_id,"Map",self.worker_list[i],map_splits[i],job.infile, partitions)
                job.map_task_list.append(task)
            #create reducer tasks
            for i in range(job.num_reducers):
                self.task_id += 1
                reduce_partitions={}
                for j in range(num_map_tasks):
                    reduce_partitions[(job.map_task_list[j]).task_id]=i
                task = Task(self.task_id,"Reduce",self.worker_list[i],reduce_partitions,None,job.outfile+"_"+i)
                job.reduce_task_list.append(task)
            #submit task
            for i in range(len(job.map_task_list)):
                client = zerorpc.Client()
                client.connect((job.map_task_list[i]).worker["address"])
                client.startMap(job.map_task_list[i])
            for i in range(len(job.reduce_task_list)):
                client = zerorpc.Client()
                client.connect((job.reduce_task_list[i]).worker["address"])
                client.startReduce(job.reduce_task_list[i])


            #splits_num = job.splits/num_map_tasks


            # if(len(job.splits) % num_map_tasks == 0 ):



       
    

if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = zerorpc.Server(Master(port,data_dir))
    addr = "tcp://0.0.0.0:"+port
    master.bind(addr)
    master.run()
    
    
    
    
