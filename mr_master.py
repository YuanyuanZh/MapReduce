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
        self.taskList = []

    def addTaskList(self,taskList):
        self.taskList += taskList

    def setStatus(self, status):
        self.status = status

    def setProgress(self, progress):
        self.progress = progress

class Task():
    def __init__(self):


class Master():
    def __init__(self, port,data_dir):
        self.port = port
        self.data_dir = data_dir
        self.worker_list = []
        self.work_id = -1
        self.job_id = -1
        self.jobs = Queue()

    def getNewJobID(self):
        self.job_id += 1
        return self.job_id

    def submitJob(self, conf):
        #create job
        job = Job(conf)
        self.jobs.put_nowait(job)
        return "job submmit sucessfully"

    def registerWorker(self, worker_address):
        self.work_id += 1
        self.worker_list.append({self.work_id,worker_address})
        return self.work_id

    def jobScheduler(self):
        while not self.jobs.empty():
            job = self.jobs.get()
            #creat tasks
            num_map_tasks = len(self.worker_list)
            map_tasks = []
            splits_num = job.splits/num_map_tasks
            if(len(job.splits) % num_map_tasks == 0 ):
                for i in range()


       
    

if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = zerorpc.Server(Master(port,data_dir))
    addr = "tcp://0.0.0.0:"+port
    master.bind(addr)
    master.run()
    
    
    
    
