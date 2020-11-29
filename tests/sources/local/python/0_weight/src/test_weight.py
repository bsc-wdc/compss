# For better print formatting
from __future__ import print_function

# Imports
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.api import compss_barrier
from pycompss.api.parameter import *

import time

CONTENT="Content"

RENAMED_SUFFIX=".IT"
@constraint(computing_units="2")
@task(filename1=FILE_OUT, filename2=FILE_OUT, filename3=FILE_OUT)
def gen_task1(filename1, filename2, filename3, text):
    f = open(filename1, "w")
    f.write(text)
    f.close()
    f = open(filename2, "w")
    f.write(text)
    f.close()
    f = open(filename3, "w")
    f.write(text)
    f.close()

@constraint(memory_size="700")
@task(filename1=FILE_OUT, filename2=FILE_OUT, filename3=FILE_OUT)
def gen_task1(filename1, filename2, filename3, text):
    f = open(filename1, "w")
    f.write(text)
    f.close()
    f = open(filename2, "w")
    f.write(text)
    f.close()
    f = open(filename3, "w")
    f.write(text)
    f.close()

@task(filename1={Type:FILE_IN, Weight:"3.0"}, filename2=FILE_IN, filename3=FILE_IN)
def read_task1(filename1, filename2, filename3):
    print("File path is :" + filename1)
    if not filename1.startswith("/tmp/COMPSsWorker01"):
        raise Exception("Incorrect filename. Renamed file name")

@task(filename1={Type:FILE_IN, Weight:"3.0"}, filename2=FILE_IN, filename3=FILE_IN)
def read_task2(filename1, filename2, filename3):
    print("File path is :" + filename1)
    if not filename1.startswith("/tmp/COMPSsWorker02"):
        raise Exception("Incorrect filename. Renamed file name")



def main():
    time.sleep(25)
    gen_task1("filename1_1", "filename1_2", "filename1_3", CONTENT)
    gen_task1("filename2_1", "filename2_2", "filename2_3", CONTENT)
    compss_barrier()
    read_task2("filename2_1", "filename1_2", "filename1_3")
    read_task1("filename1_1", "filename2_2", "filename2_3")

if __name__ == "__main__":
    main()
