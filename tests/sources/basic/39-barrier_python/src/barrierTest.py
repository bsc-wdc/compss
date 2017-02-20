"""
@author: fconejer

PyCOMPSs Constraints test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the constraints, and constraints with environment variables.
"""
        
from pycompss.api.api import compss_wait_on, barrier
from tasks import get_hero
import time       

def main_program():
    results = []
    for i in xrange(20):
        results.append(get_hero())
        
    print "All tasks submitted --> waiting."
    start_time = time.time()
    barrier()
    elapsed_time = time.time() - start_time
    print "Finished waiting"
    print "Elapsed time: ", elapsed_time
    
    if elapsed_time >= 1:
        print("- Test barrier: OK")
    else:
        print("- Test barrier: ERROR")
                
if __name__ == "__main__":
    main_program()

