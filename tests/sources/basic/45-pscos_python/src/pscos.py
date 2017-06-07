from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.storage.api import getByID

from models import mySO
from models import Words, Result
from models import InputData

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
	o.deletePersistent()
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
    print "XXXXXXXXXXXXXXXXXX"
    print final.myd
    print "XXXXXXXXXXXXXXXXXX"
    result = final.get()

    if result['This'] == 4 and result['is'] == 4 and result['a'] == 4 and result['test'] == 4:
        print "- Python Wordcount 2 with PSCOs: OK"
        return True
    else:
        print "- Python Wordcount 2 with PSCOs: ERROR"
        return False


@task(o2=INOUT)
def transform1(o1, o2):
        pow2 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value * value)
                pow2[k] = l
        o2.set(pow2)
        print "Function: Pow 2."
        print "Transformation 1 result in o2: ", o2.get()

@task(o2=INOUT)
def transform2(o1, o2):
        add1 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value + 1)
                add1[k] = l
        o2.set(add1)
        print "Function: Add 1."
        print "Transformation 2 result in o2: ", o2.get()

@task(returns=InputData)
def transform3(o1, o2):
        mult3 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value * 3)
                mult3[k] = l
        o2.set(mult3)
        print "Function: Multiply per 3."
        print "Transformation 3 result in o2: ", o2.get()
        return o2


def TiramisuMockup():
        from pycompss.api.api import compss_wait_on
        '''
        Tiramisu Mockup Test
        '''
        myobj = InputData()
        myobj.set({'first':[1,1,1,1], 'second':[2,2,2,2], 'third':[3,3,3,3], 'fourth':[4,4,4,4]})
        out1 = InputData()
        out2 = InputData()
        out3 = InputData()
        myobj.makePersistent()
        out1.makePersistent()
        out2.makePersistent()
        out3.makePersistent()

        result = ''
        transform1(myobj, out1)
        transform2(out1, out2)
        result = transform3(out2, out3)

        result = compss_wait_on(result)
        outTrans1 = compss_wait_on(out1)
        outTrans2 = compss_wait_on(out2)
        outTrans3 = compss_wait_on(out3)

        '''
        print "OUTPUTS:"
        print "Transformation 1: ", outTrans1.get()
        print "Transformation 2: ", outTrans2.get()
        print "Transformation 3: ", outTrans3.get()
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print "RESULT: ", result.get()
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        '''

        finalResults = result.get()
        if all(x == 6 for x in finalResults['first']) and all(x == 15 for x in finalResults['second']) and all(x == 30 for x in finalResults['third']) and all(x == 51 for x in finalResults['fourth']):
                print "- Python Tiramisu Mockup with PSCOs: OK"
		return True
	else:
		print "- Python Tiramisu Mockup with PSCOs: ERROR"
		return False


@task(o2=OUT)
def transform1_2(o1, o2):
        pow2 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value * value)
                pow2[k] = l
        o2.set(pow2)
        print "Function: Pow 2."
        print "Transformation 1 result in o2: ", o2.get()

@task(o2=OUT)
def transform2_2(o1, o2):
        add1 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value + 1)
                add1[k] = l
        o2.set(add1)
        print "Function: Add 1."
        print "Transformation 2 result in o2: ", o2.get()

@task(returns=InputData, o2=OUT)
def transform3_2(o1, o2):
        mult3 = {}
        images = o1.get()
        for k,v in images.iteritems():
	        l = []
	        for value in v:
		      l.append(value * 3)
                mult3[k] = l
        o2.set(mult3)
        print "Function: Multiply per 3."
        print "Transformation 3 result in o2: ", o2.get()
        return o2



def TiramisuMockup2():
        from pycompss.api.api import compss_wait_on
        '''
        Tiramisu Mockup Test 2
        '''
        myobj = InputData()
        myobj.set({'first':[1,1,1,1], 'second':[2,2,2,2], 'third':[3,3,3,3], 'fourth':[4,4,4,4]})
        out1 = InputData()
        out2 = InputData()
        out3 = InputData()
        myobj.makePersistent()
        out1.makePersistent()
        out2.makePersistent()
        out3.makePersistent()

        result = ''
        transform1_2(myobj, out1)
        transform2_2(out1, out2)
        result = transform3_2(out2, out3)

        result = compss_wait_on(result)

        import time
        time.sleep(60)

        outTrans1 = compss_wait_on(out1)
        outTrans2 = compss_wait_on(out2)
        outTrans3 = compss_wait_on(out3)

        print "OUTPUTS:"
        print "Transformation 1: ", outTrans1.get()
        print "Transformation 2: ", outTrans2.get()
        print "Transformation 3: ", outTrans3.get()
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        print "RESULT: ", result.get()
        print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

        finalResults = result.get()
        if all(x == 6 for x in finalResults['first']) and all(x == 15 for x in finalResults['second']) and all(x == 30 for x in finalResults['third']) and all(x == 51 for x in finalResults['fourth']):
                print "- Python Tiramisu Mockup 2 with PSCOs: OK"
		return True
	else:
		print "- Python Tiramisu Mockup 2 with PSCOs: ERROR"
		return False



def main():
	results = {}
	#results['basic'] =  basicTest()
	#results['basic2'] =  basic2Test()
	#results['wordcount'] = WordCount()
	results['wordcount2'] = WordCount2()
	#results['tiramisu'] = TiramisuMockup()
	#results['tiramisu2'] = TiramisuMockup2()
	if all(x for x in results.values()):
	    print "- PSCOs TEST FINISHED SUCCESSFULLY."
	else:
	    print "- PSCOs TEST FINISHED WITH ERRORS."

if __name__ == '__main__':
    main()
