from genericpath import exists
import numpy as np

from sklearn import clone, datasets
from sklearn.utils import shuffle
import time

from pycompss.runtime.management.classes import Future
import time
import sys
import os
from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.constraint import constraint
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_delete_file
from pycompss.api.parameter import *


TEST_FILE_PATH = "/tmp/in_out_objects_file_python/"

MATRIX_SIZE_Y = 5
MATRIX_SIZE_X = 5


def print_mat(matrix, label):
    print("printing matrix "+label+":")
    print(matrix[0][0])
    print("------------------------------------", flush=True)


def create_mat(value):
    return [[np.full( (MATRIX_SIZE_Y, MATRIX_SIZE_X), value)]]


@constraint(processor_architecture = "processor_ag_3")
@task(
    matA={Type: COLLECTION_IN},
    returns={Type: COLLECTION_OUT}
)
def nested_in_return(matA, label):
    print_mat(matA, "input " + label)
    current_value = matA[0][0][0][0]
    matC = create_mat(current_value+1)
    print_mat(matC, "output " + label)
    return matC

@constraint(processor_architecture = "processor_ag_2")
@task(
    matA={Type: COLLECTION_IN},
    returns={Type: COLLECTION_OUT}
)
def in_return(matA):
    res = nested_in_return(matA, "nested in_return") #this does not work unless a compss_wait_on is added
    return res


@constraint(processor_architecture = "processor_ag_2")
@task(
    matA={Type: COLLECTION_IN},
    returns={Type: COLLECTION_OUT}
)
def in_return_w_print(matA):
    print_mat(matA, "input in_return_w_print")
    matC = nested_in_return(matA, "nested_in_return_w_print")
    # time.sleep(5)
    matC = compss_wait_on(matC)
    print_mat(matC, "output in_return_w_print")
    return matC



@constraint(processor_architecture = "processor_ag_3")
@task(
    matC=COLLECTION_INOUT
)
def nested_inout(matC, label):
    mat = matC[0][0]
    print_mat(matC, "input " + label)

    for i in range(len(mat)):
        for j in range(len(mat[i])):
            mat[i][j] += 1

    print_mat(matC, "output " + label)

@constraint(processor_architecture = "processor_ag_2")
@task(
    matC=COLLECTION_INOUT
)
def inout(matC):
    nested_inout(matC, "nested_inout")

@constraint(processor_architecture = "processor_ag_2")
@task(
    matC=COLLECTION_INOUT
)
def inout_w_print(matC):
    print_mat(matC, "input inout_w_print")
    nested_inout(matC, "nested_inout_w_print")
    matC_res = compss_wait_on(matC)
    print_mat(matC_res, "output inout_w_print")


@constraint(processor_architecture = "processor_ag_3")
@task()
def print_task(matC, label):
    print_mat(matC, label)


@constraint(processor_architecture = "processor_ag_3")
@task(
    returns={Type: COLLECTION_OUT}
)
def nested_generation_return():
    matC = create_mat(30)
    print_mat(matC, "output nested_generation_return")
    return matC

@constraint(processor_architecture = "processor_ag_2")
@task(
    returns={Type: COLLECTION_OUT}
)
def generation_return():
    return nested_generation_return() #this does not work unless a compss_wait_on is added


@constraint(processor_architecture = "processor_ag_2")
@task(
    matC=COLLECTION_INOUT
)
def consumption(matC, label):
    print_task(matC, label)



@constraint(processor_architecture = "processor_ag_3")
@task(
    matC=COLLECTION_INOUT
)
def nested_generation_inout(matC):
    print_mat(matC, "input nested_generation_inout")

    mat = matC[0][0]
    for i in range(len(mat)):
        for j in range(len(mat[i])):
            mat[i][j] += 1

    print_mat(matC, "output nested_generation_inout")

@constraint(processor_architecture = "processor_ag_2")
@task(
    matC=COLLECTION_INOUT
)
def generation_inout(matC):
    nested_generation_inout(matC)



def main():

    matA = create_mat(0)
    matC = in_return(matA)
    matC = compss_wait_on(matC)
    print_mat(matC, "main result in_return")

    matA = create_mat(10)
    matC = in_return_w_print(matA) 
    matC = compss_wait_on(matC)
    print_mat(matC, "main result in_return_w_print")


    matC = create_mat(20)
    
    inout_w_print(matC)
    matC = compss_wait_on(matC)
    print_mat(matC, "main result inout_w_print")


    inout(matC)
    matC = compss_wait_on(matC)
    print_mat(matC, "main result inout")


    matC = generation_return()
    consumption(matC, "consumption_return")
    
    generation_inout(matC)
    consumption(matC, "consumption_inout")
    
    matC = compss_wait_on(matC)
    print_mat(matC, "end in main")





@task()
def main_agents():
    main()



if __name__ == '__main__':
    main()