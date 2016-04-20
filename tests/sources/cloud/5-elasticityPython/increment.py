#
#  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

FILENAME="file"
INITIAL_VALUE=1

def main_program():
    # Check and get parameters
    if len(sys.argv) != 3:
        usage()
        exit(-1) 
    INITIAL_VALUE = int(sys.argv[1])
    numTasks = int(sys.argv[2])
    
    # Initialize counter files
    for i in range(numTasks):
        initializeCounter(FILENAME + str(i))
    	printCounterValue(FILENAME + str(i), i)
      
    # Execute increment
    for i in range(numTasks):
        increment(FILENAME + str(i))
    
    # Write final counters state (sync)
    print "Final counter values:"
    for i in range(numTasks):
        printCounterValue(FILENAME + str(i), i)
    

def usage():
    print "[ERROR] Bad numnber of parameters"
    print "    Usage: increment.py <counterValue> <numberOfTasks> <minVM> <maxVM> <creationTime>"

def initializeCounter(filePath):
    # Write value counter
    fos = open(filePath, 'w')
    fos.write(str(INITIAL_VALUE))
    fos.close()
    
def printCounterValue(filePath, pos):
    from pycompss.api.api import compss_open
    
    # Read value counter 1
    fis = compss_open(filePath, 'r+')
    counter = fis.read()
    fis.close()
    
    # Print values
    print "- Counter" + str(pos) + " value is " + counter

@constraint(ProcessorCoreCount=2)
@task(filePath = FILE_INOUT)
def increment(filePath):
    # Read value
    fis = open(filePath, 'r')
    value = fis.read()
    fis.close()

    # Sleep to increase task size
    import time
    time.sleep( 30 ) #Seconds

    # Write value
    fos = open(filePath, 'w')
    fos.write(str(int(value) + 1))
    fos.close()


if __name__ == "__main__":
    main_program()
    
