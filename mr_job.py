import zerorpc
import sys

class job(object):
    def __init__(self, classname,split_size,num_redurcers,infile,outfile):
        self.classname = classname
        self.split_size = split_size
        self.num_redurcers = num_redurcers
        self.infile = infile
        self.outfile = outfile
    def submitJobInternal(self):
        jobClient = zerorpc.Client()
        jobClient.connect(master_addr)
        jobID = jobClient.getNewJobID()
        print jobID
        print jobClient.submitJob()
        
        
if __name__ == '__main__':
    master_addr = sys.argv[1]
    mr_class_name = sys.argv[2]
    split_size = sys.argv[3]
    num_reducers = sys.argv[4]
    input_file = sys.argv[5]
    output_file = sys.argv[6]
    
    jobClient = job(mr_class_name,split_size,num_reducers,input_file,output_file)
    jobClient.submitJobInternal()
    