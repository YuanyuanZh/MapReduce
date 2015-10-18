import zerorpc
import gevent
import socket

class Worker():

    def __init__(self, master_address ,worker_address=None):
        self.master_address = master_address
        if(worker_address == None):
            self.worker_address = self.getMyAddress()
            self.is_remote = False
        else:
            self.worker_address = worker_address
            self.is_remote = True

    def mapper(self):
        print('running in mapper')
        gevent.sleep(0)
        print('Explicit context switch to mapper again')

    def reducer(self):
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
            addr = "0.0.0.0"+self.worker_address.split(":")[1]
        #addr = "tcp://0.0.0.0:"+port
        master.bind(addr)
        master.run()

    def register(self):
        client = zerorpc.Client()
        client.connect(self.master_address)
        id = client.registerWorker(self.worker_address)

    def run(self):
        self.register()
        self.startRPCServer()
        gevent.joinall([
            gevent.spawn(self.mapper),
            gevent.spawn(self.reducer),
        ])

