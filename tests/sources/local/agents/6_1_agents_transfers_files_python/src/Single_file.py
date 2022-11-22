from genericpath import exists
import numpy as np

from sklearn import clone, datasets
from sklearn.utils import shuffle
import time

import dislib as ds
from dislib.classification import CascadeSVM, RandomForestClassifier
from dislib.cluster import DBSCAN, KMeans, GaussianMixture
from dislib.decomposition import PCA
from dislib.neighbors import NearestNeighbors
from dislib.preprocessing import StandardScaler
from dislib.recommendation import ALS
from dislib.regression import LinearRegression
from dislib.model_selection import GridSearchCV, KFold
from pycompss.runtime.management.classes import Future
import time
import sys
import os
from pycompss.api.task import task
from pycompss.api.api import compss_wait_on_file
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_delete_file
from pycompss.api.parameter import *


TEST_FILE_PATH = "/tmp/in_out_objects_file_python/"

MATRIX_SIZE_Y = 5
MATRIX_SIZE_X = 5


def print_mat(path, label):
    print("printing matrix "+label+":")
    print(str(np.load(path)))
    print("------------------------------------", flush=True)





def fill_with(path, value):
    block = np.full( (MATRIX_SIZE_Y, MATRIX_SIZE_X), value)
    np.save(path, block)




@task(
    pathA={Type: FILE_IN},
    pathC={Type: FILE_OUT}
)
def nested_in_return(pathA, pathC, label):
    print_mat(pathA, "input " + label)
    current_value = np.load(pathA)[0][0]
    fill_with(pathC, current_value+1)
    print_mat(pathC, "output " + label)

@task(
    pathA={Type: FILE_IN},
    pathC={Type: FILE_OUT},
    on_failure= 'FAIL'
)
def in_return(pathA, pathC):
    nested_in_return(pathA, pathC, "nested in_return")


@task(
    pathA={Type: FILE_IN},
    pathC={Type: FILE_OUT},
    on_failure= 'FAIL'
)
def in_return_w_print(pathA, pathC):
    print_mat(pathA, "input in_return_w_print")
    nested_in_return(pathA, pathC, "nested_in_return_w_print")
    # time.sleep(5)
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "output in_return_w_print")



@task(
    pathC={Type: FILE_INOUT}
)
def nested_inout(pathC, label):
    print_mat(pathC, "input " + label)

    mat = np.load(pathC)
    for i in range(len(mat)):
        for j in range(len(mat[i])):
            mat[i][j] += 1
    np.save(pathC, mat)

    print_mat(pathC, "output " + label)

@task(
    pathC={Type: FILE_INOUT},
)
def inout(pathC):
    nested_inout(pathC, "nested_inout")

@task(
    pathC={Type: FILE_INOUT},
)
def inout_w_print(pathC):
    print_mat(pathC, "input inout_w_print")
    nested_inout(pathC, "nested_inout_w_print")
    pathC = compss_wait_on_file(pathC)  
    print_mat(pathC, "output inout_w_print")




@task(
    pathC={Type: FILE_IN}
)
def print_task(pathC, label):
    print_mat(pathC, label)



@task(
    pathC={Type: FILE_OUT}
)
def nested_generation_return(pathC):
    fill_with(pathC, 30)
    print_mat(pathC, "output nested_generation_return")

@task(
    pathC={Type: FILE_OUT}
)
def generation_return(pathC):
    nested_generation_return(pathC)


@task(
    pathC={Type: FILE_IN}
)
def consumption(pathC, label):
    print_task(pathC, label)



@task(
    pathC={Type: FILE_INOUT}
)
def nested_generation_inout(pathC):
    print_mat(pathC, "input nested_generation_inout")

    mat = np.load(pathC)
    for i in range(len(mat)):
        for j in range(len(mat[i])):
            mat[i][j] += 1
    np.save(pathC, mat)

    print_mat(pathC, "output nested_generation_inout")

@task(
    pathC={Type: FILE_INOUT}
)
def generation_inout(pathC):
    nested_generation_inout(pathC)




@task(
    pathC={Type: FILE_OUT}
)
def gen_mat(pathC):
    time.sleep(5)
    fill_with(pathC, 8)
    print_mat(pathC, "output task")










def main():
    pathA1 = TEST_FILE_PATH+"A1.npy"
    pathA2 = TEST_FILE_PATH+"A2.npy"
    pathC = TEST_FILE_PATH+"C.npy"

    if not os.path.exists(TEST_FILE_PATH):
        os.mkdir(TEST_FILE_PATH)

    if os.path.exists(pathC):
        os.remove(pathC)


    fill_with(pathA1, 0)
    in_return(pathA1, pathC)
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "main result in_return")

    fill_with(pathA2, 10)
    in_return_w_print(pathA2, pathC) 
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "main result in_return_w_print")


    fill_with(pathC, 20)
    inout_w_print(pathC)
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "main result inout_w_print")

    inout(pathC)
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "main result inout")


    generation_return(pathC)
    consumption(pathC, "consumption_return")
    
    generation_inout(pathC)
    consumption(pathC, "consumption_inout")
    
    pathC = compss_wait_on_file(pathC)
    print_mat(pathC, "end in main")
    





@task()
def main_agents():
    main()



if __name__ == '__main__':
    main()