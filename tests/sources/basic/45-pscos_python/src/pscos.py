from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.storage.api import getByID

from models import mySO
from models import Words, Result

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
def simpleTask(r):
    val = r.get()
    print "R: ", r
    print "R.get = ", val
    return val * val


@task(returns=mySO)
def complexTask(p):
    v = p.get()
    print "P: ", p
    print "P.get: ", v
    q = mySO(v*10)
    q.makePersistent()
    print "Q: ", q
    print "Q.get = ", q.get()
    return q


@task(returns = dict)
def wordcountTask(words):
    partialResult = {}
    data = words.get()
    for entry in data.split():
        if entry in partialResult:
            partialResult[entry] += 1
        else:
            partialResult[entry] = 1
    return partialResult


@task(dic1=INOUT)
def reduceTask(dic1, dic2):
    for k in dic2:
        if k in dic1:
            dic1[k] += dic2[k]
        else:
            dic1[k] = dic2[k]


@task(final=INOUT)
def reduceTaskPSCOs(final, dic2):
    dic1 = final.get()
    for k in dic2:
        if k in dic1:
            dic1[k] += dic2[k]
        else:
            dic1[k] = dic2[k]
    final.set(dic1)


def basicTest():
	'''
	This Test:
	- Instantiates a SCO.
	- Makes it persistent.
	- Calls a task with that PSCO -> IN parameter.
	- Waits for the task result.
	- Deletes the PSCO.
	'''
	from pycompss.api.api import compss_wait_on
	o = mySO(10)
	print "BEFORE MAKEPERSISTENT: o.id: ", o.getID()
	# Pesist the object to disk (it will be at /tmp/uuid.PSCO)
	o.makePersistent()
	print "AFTER MAKEPERSISTENT:  o.id: ", o.getID()
	v = simpleTask(o)
	v1 = compss_wait_on(v)
	# Remove the persisted object from disk (from /tmp/uuid.PSCO)
	o.delete()
	if v1 == 100:
	    print "- Simple Test Python PSCOs: OK"
	    return True
	else:
	    print "- Simple Test Python PSCOs: ERROR"
	    return False


def basic2Test():    
	'''
	This Test:
	    - Instantiates a SCO.
	    - Makes it persistent.
	    - Calls a task with that PSCO -> IN parameter.
	        - The task receives the PSCO and gets its value.
	        - Instantiates another SCO and makes it persistent within the task.
	        - Returns the new PSCO.
	    - Calls another task with the input from the first one.
	        - Gets the output PSCO and receives it as input.
	    - Waits for the first task result.
	    - Waits for the second task result.
	'''
	from pycompss.api.api import compss_wait_on
	p = mySO(1)
	p.makePersistent()
	x = complexTask(p)
	y = simpleTask(x)
	res1 = compss_wait_on(x)
	res2 = compss_wait_on(y)
	# print "O.get must be  1 = ", p.get()
	# print "X.get must be 10 = ", res1.get()
	# print "Y must be    100 = ", res2
	if p.get() == 1 and res1.get() == 10 and res2 == 100:
	    print "- Complex Test Python PSCOs: OK"
	    return True
	else:
	    print "- Complex Test Python PSCOs: ERROR"
	    return False


def WordCount():
	from pycompss.api.api import compss_wait_on
	'''
	WordCount Test
	    - Wordcount task receives a PSCO and returns a dictionary.
	    - Reduce task works with python dictionaries.
	'''
	words = [Words('This is a test'), Words('This is a test'), Words('This is a test'), Words('This is a test')]
	for w in words:
	    w.makePersistent()
	result = {}

	localResults = []
	for w in words:
	    partialResults = wordcountTask(w)
	    reduceTask(result, partialResults)
	result = compss_wait_on(result)
	
	if result['This'] == 4 and result['is'] == 4 and result['a'] == 4 and result['test'] == 4:
		print "- Python Wordcount 1 with PSCOs: OK"
		return True
	else:
		print "- Python Wordcount 1 with PSCOs: ERROR"
		return False


def WordCount2():
	from pycompss.api.api import compss_wait_on
	'''
	WordCount Test
	    - Wordcount task receives a PSCO and returns a dictionary.
	    - Reduce task receives a INOUT PSCO (result) where accumulates the partial results.
	'''
	words = [Words('This is a test'), Words('This is a test'), Words('This is a test'), Words('This is a test')]
	for w in words:
	    w.makePersistent()
	result = Result()
	result.makePersistent()

	localResults = []
	for w in words:
	    partialResults = wordcountTask(w)
	    reduceTaskPSCOs(result, partialResults)
	final = compss_wait_on(result)

	result = final.get()

	if result['This'] == 4 and result['is'] == 4 and result['a'] == 4 and result['test'] == 4:
		print "- Python Wordcount 2 with PSCOs: OK"
		return True
	else:
		print "- Python Wordcount 2 with PSCOs: ERROR"
		return False


def main():
	results = {}
	results['basic'] =  basicTest()
	results['basic2'] =  basic2Test()
	results['wordcount'] = WordCount()
	results['wordcount2'] = WordCount2()
	if all(x for x in results.values()):
	    print "- PSCOs TEST FINISHED SUCCESSFULLY."
	else:
		print "- PSCOs TEST FINISHED WITH ERRORS."

if __name__ == '__main__':
    main()
