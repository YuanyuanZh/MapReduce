import zerorpc
import sys
import os
# import input_split
import gevent
import time

class JobClient(object):
    def __init__(self, master_addr,classname,split_size,num_redurcers,infile,outfile):
        self.master_addr = master_addr
        self.classname = classname
        self.split_size = split_size
        self.num_redurcers = num_redurcers
        self.infile = infile
        self.outfile = outfile

    # def splitInput(self):
    #     split_hashmap = input_split.Split(self.infile,self.split_size,self.classname).generate_split_info()
    #     return split_hashmap

    def submitJobInternal(self):
        jobClient = zerorpc.Client()
        jobClient.connect('tcp://'+ self.master_addr)
        job_id = jobClient.getNewJobID()
        #compute splits
        #splits = self.splitInput()
        #store job configuration
        conf = {
            'jobId': job_id,
            'className': self.classname,
            'split_size': self.split_size,
            #'splits': splits,
            'num_reducers': self.num_reducers,
            'infile':self.infile,
            'outfile': self.outfile
        }
        print "Get Job id: %d at %s" %(job_id,time.asctime( time.localtime(time.time()) ))
        #check input and output
        e = os.path.exists(self.infile)
        if e == False:
            raise IOError,"No input file"
        jobClient.submitJob(conf)
        return job_id

    def run(self):
        job_id = self.submitJobInternal()
        while True:
            gevent.sleep(5)
            client = zerorpc.Client()
            client.connect('tcp://'+ self.master_addr)
            state,progress = client.getJobStatus(job_id)
            print 'Job %s state is : %s' %(self.classname, state)
            print 'Job %s progress is: %s' %(self.classname, progress)
            if state == 'COMPLETE':
                print 'Please get result at %s : %s' %(self.master_addr, self.outfile)
                break


        
        
if __name__ == '__main__':
    master_addr = sys.argv[1]
    mr_class_name = sys.argv[2]
    split_size = sys.argv[3]
    num_reducers = sys.argv[4]
    input_file = sys.argv[5]
    output_file = sys.argv[6]
    
    jobClient = JobClient(master_addr,mr_class_name,split_size,num_reducers,input_file,output_file)
    jobClient.run()

    