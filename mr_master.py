import zerorpc
import sys

class mr_master():
    def __init__(self, port,data_dir):
        self.port = port
        self.data_dir = data_dir
    def getNewJobID(self):
        return 0
    def submitJob(self):
        return "job submmit sucessfully"
       
    

if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = zerorpc.Server(mr_master(port,data_dir))
    addr = "tcp://0.0.0.0:"+port
    master.bind(addr)
    master.run()
    
    
    
    
