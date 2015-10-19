import zerorpc
import sys
import os
import input_split

class JobClient(object):
    def __init__(self, classname,split_size,num_redurcers,infile,outfile):
        self.classname = classname
        self.split_size = split_size
        self.num_redurcers = num_redurcers
        self.infile = infile
        self.outfile = outfile

    def splitInput(self):
        split_hashmap = input_split.Split(self.infile,self.split_size,self.classname).generate_split_info()
        return split_hashmap

    def submitJobInternal(self):
        jobClient = zerorpc.Client()
        jobClient.connect(master_addr)
        jobID = jobClient.getNewJobID()
        #compute splits
        splits = self.splitInput()
        #store job configuration
        conf = {
            'jobId': jobID,
            'className': self.classname,
            'split_size': self.split_size,
            'splits': splits,
            'num_reducers': self.num_reducers,
            'infile':self.infile,
            'outfile': self.outfile
        }
        print jobID
        #check input and output
        e = os.path.exists(self.infile)
        if e == False:
            raise IOError,"No input file"
        ret =jobClient.submitJob(conf)
        
        
if __name__ == '__main__':
    master_addr = sys.argv[1]
    mr_class_name = sys.argv[2]
    split_size = sys.argv[3]
    num_reducers = sys.argv[4]
    input_file = sys.argv[5]
    output_file = sys.argv[6]
    
    jobClient = JobClient(mr_class_name,split_size,num_reducers,input_file,output_file)
    jobClient.submitJobInternal()
    