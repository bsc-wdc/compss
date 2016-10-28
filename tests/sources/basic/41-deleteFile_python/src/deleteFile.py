"""
@author: fconejer

PyCOMPSs Delete File test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the delete file functionality.
"""
        
from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_open
from pycompss.api.api import compss_delete
from tasks import increment, increment2

def main_program():
    # Check and get parameters
    initialValue = "1"
    counterName = "counter_INOUT"
    counterNameIN = "counter_IN"   # check that this file does not exist after the execution
    counterNameOUT = "counter_OUT" # check that this file does not exist after the execution

    for i in range(3):
        # Write value
        fos = open(counterName, 'w')
        fos2 = open(counterNameIN, 'w')
        fos.write(initialValue)
        fos2.write(initialValue)
        fos.close()
        fos2.close()
        print "Initial counter value is " + initialValue
        # Execute increment
        increment(counterName)
        increment2(counterNameIN, counterNameOUT)
        # Read new value
        print "After sending task"
        if i == 0:
            compss_delete(counterName)
        compss_delete(counterNameIN)
        compss_delete(counterNameOUT)

    fis = compss_open(counterName, 'r+')
    finalValue = fis.read()
    fis.close()
    print "Final counter value is " + finalValue
    compss_delete(counterName)



if __name__ == "__main__":
    main_program()
