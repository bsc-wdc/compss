from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.storage.Object import SCO

class mySO(SCO):
    value = 0
    def __init__(self, v):
        self.value = v
        
    def get(self):
        return self.value
      
    def put(self, v):
        self.value = v


@task(returns=int)
def myTask(p):
    print "P: ", p
    val = p.get()
    return val * val


def main():
    from pycompss.api.api import compss_wait_on
    
    o = mySO(10)
    print "BEFORE MAKEPERSISTENT: o.id: ", o.getID()
    # Pesist the object to disk (it will be at /tmp/uuid.PSCO)
    o.makePersistent()
    print "AFTER MAKEPERSISTENT:  o.id: ", o.getID()
    
    v = myTask(o)
    
    v1 = compss_wait_on(v)
    
    # Remove the persisted object from disk (from /tmp/uuid.PSCO)
    o.delete()
    
    if v1 == 100:
        print "- Test Python PSCOs: OK"
    else:
        print "- Test Python PSCOs: ERROR"
    

if __name__ == '__main__':
    main()
    
