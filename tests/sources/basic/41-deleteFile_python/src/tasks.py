"""
@author: fconejer
  
PyCOMPSs Delete File Testbench Tasks
====================================
"""
    
from pycompss.api.task import task
from pycompss.api.parameter import *
  
@task(counterFile = FILE_INOUT)
def increment(counterFile):
    # Read value
    fis = open(counterFile, 'r')
    value = fis.read()
    fis.close()
    # Write value
    fos = open(counterFile, 'w')
    fos.write(str(int(value) + 1))
    fos.close()

@task(counterFileIn = FILE_IN, counterFileOut = FILE_OUT)
def increment2(counterFileIn, counterFileOut):
    # Read value
    fis = open(counterFileIn, 'r')
    value = fis.read()
    fis.close()
    # Write value
    fos = open(counterFileOut, 'w')
    fos.write(str(int(value) + 1))
    fos.close()
