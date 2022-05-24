#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_delete_file, compss_file_exists, compss_open, compss_wait_on, compss_wait_on_file, compss_open, TaskGroup
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

import time
import random
import os
from os import path

HELLO="Hello World!"
WRITTING="Writting structure"

@task(structure=FILE_OUT)
def gen_file(structure):
    f = open(structure, "w")
    f.write(WRITTING)
    f.close()
    time.sleep(5.0)

def main():
    hello_file="hello_world.txt"
    f= open(hello_file, 'w')
    f.write(HELLO)
    f.close()

    #Check file_exists after creation
    if (not compss_file_exists(hello_file)):
        raise Exception("Wait on not used object not working")
    print("compss_file_exists in existing file is CORRECT.")

    #Check wait on not used object
    a = 20
    a = compss_wait_on(a)
    if ( a != 20):
        raise Exception(" Wait on not used object not working (" +str(a)+")")
    print("Wait on not used object is CORRECT.")

    #Check wait on not used file
    compss_wait_on_file(hello_file)
    f = compss_open(hello_file)
    content = f.read()
    f.close()
    if (content != HELLO):
        raise Exception(" Wait on not used file is not working (" + content + ")")
    print("Wait on not used file is CORRECT.")

    # Check delete_file in not used file
    compss_delete_file(hello_file)

    struct_file="structure.pdb"
    struct_file_2="structure2.pdb"
    #Check file exist before existing file
    if (compss_file_exists(struct_file)):
        raise Exception("Struct file shouldn't exists and it is existing")
    print("compss_file_exists in no existing file is CORRECT.")
    gen_file(struct_file)

    #Check file exist after executint the task
    if (not compss_file_exists(struct_file)):
        raise Exception("Struct file should exists and it is not existing")
    print("compss_file_exists in used file is existing file is CORRECT.")

    #Check file exist after delete but task not finished
    compss_delete_file(struct_file)
    if (compss_file_exists(struct_file)):
        raise Exception("Struct file shouldn't exists after_delete and it is existing")

    print("compss_exists_file after delete is CORRECT")

    gen_file(struct_file_2)
    compss_wait_on_file(struct_file_2)
    f= open(struct_file_2)
    content = f.read()
    f.close()
    if (not compss_file_exists(struct_file_2)):
        raise Exception("Struct file 2 should exists and it is not existing")
    print("compss_exists_file after wait_on is CORRECT")

    if (content != WRITTING):
        raise Exception(" Wait on not used file is not working (" + content + ")")

    compss_delete_file(struct_file_2)
    if (compss_file_exists(struct_file_2)):
        raise Exception("Struct file 2 shouldn't exists and it is existing")
    print("compss_exists_file after delete and wait_on is CORRECT")

    base_all_files = [hello_file, (struct_file, struct_file_2)]
    all_files = compss_wait_on_file(base_all_files)
    if all_files != base_all_files:
        raise Exception("compss_wait_on_file for multiple files failed.")
    all_files_exist = compss_file_exists(all_files)
    if all_files_exist != [False, (False, False)]:
        raise Exception("compss_file_exist for multiple files failed.")

if __name__ == '__main__':
    main()
