"""
PyCOMPSs Constraints test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the constraints, and constraints with environment variables.
"""

from pycompss.api.api import compss_wait_on
from tasks import constrained_func


def main_program():
    print "Test constraints"
    v = 10
    o = constrained_func(v)
    o = compss_wait_on(o)
    print "Test result = ", o
    if (o == v*v*v):
        print("- Function as a parameter: OK")
    else:
        print("- Function as a parameter: ERROR")



if __name__ == "__main__":
    main_program()
