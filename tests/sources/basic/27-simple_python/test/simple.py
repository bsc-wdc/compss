from pycompss.api.parameter import *
from pycompss.api.task import task

def main_program():
    from pycompss.api.api import compss_open

    # Check and get parameters
    if len(sys.argv) != 2:
        usage()
        exit(-1) 
    initialValue = sys.argv[1]
    fileName="counter"
    
    # Write value
    fos = open(fileName, 'w')
    fos.write(initialValue)
    fos.close()
    print "Initial counter value is " + initialValue
    
    # Execute increment
    increment(fileName)
    
    # Write new value
    fis = compss_open(fileName, 'r+')
    finalValue = fis.read()
    fis.close()
    print "Final counter value is " + finalValue
 
@task(filePath = FILE_INOUT)
def increment(filePath):
    print "Init task user code"
    # Read value
    fis = open(filePath, 'r')
    value = fis.read()
    print "Received " + value
    fis.close()

    # Write value
    fos = open(filePath, 'w')
    newValue=str(int(value) + 1)
    print "Computed " + newValue
    fos.write(newValue)
    fos.close()

def usage():
    print "[ERROR] Bad number of parameters"
    print "    Usage: simple <counterValue>"


if __name__ == "__main__":
    main_program()
    
