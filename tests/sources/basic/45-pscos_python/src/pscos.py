from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.storage.api import getByID

from models import mySO

# # BUG: Pickling error in the dummy deserialization.
# from pycompss.storage.Object import SCO
# 
# class mySO(SCO):
#     value = 0
#     def __init__(self, v):
#         self.value = v
#         
#     def get(self):
#         return self.value
#       
#     def put(self, v):
#         self.value = v


@task(returns=int)
def myTask(r):
    val = r.get()
    print "R: ", r
    print "R.get = ", val
    return val * val


@task(returns=mySO)
def task2(p):
    v = p.get()
    print "P: ", p
    print "P.get: ", v
    q = mySO(v*10)
    q.makePersistent()
    print "Q: ", q
    print "Q.get = ", q.get()
    return q


def main():
    from pycompss.api.api import compss_wait_on

    # Simple test
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
        print "- Simple Test Python PSCOs: OK"
    else:
        print "- Simple Test Python PSCOs: ERROR"
    
    # Complex test
    p = mySO(1)
    p.makePersistent()
    x = task2(p)
    y = myTask(x)
    res1 = compss_wait_on(x)
    res2 = compss_wait_on(y)
    print "O.get must be  1 = ", p.get()
    print "X.get must be 10 = ", res1.get()
    print "Y must be    100 = ", res2
    
    if res2 == 100:
        print "- Complex Test Python PSCOs: OK"
    else:
        print "- Complex Test Python PSCOs: ERROR"
    

if __name__ == '__main__':
    main()
    
