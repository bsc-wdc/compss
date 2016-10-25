"""
@author: fconejer

PyCOMPSs Constraints test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the constraints, and constraints with environment variables.
"""
        
from pycompss.api.api import compss_wait_on, waitForAllTasks
from tasks import get_hero
import time       

def main_program():
    results = []
    for i in xrange(20):
        results.append(get_hero())
        
    print "All tasks submitted --> waiting."
    start_time = time.time()
    waitForAllTasks()
    elapsed_time = time.time() - start_time
    print "Finished waiting"
    print "Elapsed time: ", elapsed_time
    
    if elapsed_time >= 1:
        print("- Test waitForAllTasks: OK")
    else:
        print("- Test waitForAllTasks: ERROR")
                
if __name__ == "__main__":
    main_program()
