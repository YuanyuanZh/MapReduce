import zerorpc
import gevent

def mapper():
    print('running in mapper')
    gevent.sleep(0)
    print('Explicit context switch to mapper again')
    
def reducer():
    print('Explicit context to reducer')
    gevent.sleep(0)
    print('Implicit context switch back to reducer')

gevent.joinall([
    gevent.spawn(mapper),
    gevent.spawn(reducer),
])