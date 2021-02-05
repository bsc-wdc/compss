# Import Python libraries
import math

from pycompss.api.api import compss_wait_on

from utilities import UnfolderManager

def main():
    N=4
    ufManager_1 = UnfolderManager(4,2)
    ufManager_2 = UnfolderManager(8,2)
    #It should be got from groups property of the object
    list_1 = ufManager_1.UnfoldNValues_Task(2)
    #It should be got from the function parameter value
    list_2 = ufManager_2.UnfoldNValues_Task_2(N)
    #It should be specified as returns named parameter
    list_3 = ufManager_2.UnfoldNValues_Task_3(1, returns=N)
    if len(list_1) != ufManager_1.groups:
        raise Exception("Error getting from self parameter")
    if len(list_2) != N:
        raise Exception("Error getting from values parameter")
    if len(list_3) != N:
        raise Exception("Error getting from returns argument")
    list_1 = compss_wait_on(list_1)
    if (list_1[0] != 0 or list_1[1] != 1):
        raise Exception("Error values for list 1 are not the expected")
    list_2 = compss_wait_on(list_2)
    if (list_2[0] != 0 or list_2[1] != 1 or list_2[2] != 2 or list_2[3] != 3):
        raise Exception("Error values for list 2 are not the expected")
    list_3 = compss_wait_on(list_3)
    if (list_3[0] != 0 or list_3[1] != 1 or list_3[2] != 2 or list_3[3] != 3):
        raise Exception("Error values for list 3 are not the expected")

if __name__ == '__main__':
    main()
