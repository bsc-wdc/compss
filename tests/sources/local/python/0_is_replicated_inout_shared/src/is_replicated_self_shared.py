#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_delete_file, compss_open, compss_wait_on, compss_wait_on_file, compss_open
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

import time
import random
import os
from os import path

HELLO="Hello"
WRITTING="Hello, test!"

@task(structure=FILE_INOUT, is_replicated=True)
def update_file(structure, name):
    f = open(structure, "r")
    content = f.read()
    f.close()
    f = open(structure, "w")
    f.write(content + ", " + name + "!")
    f.close()
    time.sleep(2.0)

def main(): 
    hello_file="/tmp/sharedDisk/hello_world.txt"
    f= open(hello_file, 'w')
    f.write(HELLO)
    f.close()
    
    update_file(hello_file, "test")

    compss_wait_on_file(hello_file)
    f= open(hello_file)
    content = f.read()
    f.close()
    if (content != WRITTING):
        raise Exception(" Wait on not used file is not working (" + content + ")")

    compss_delete_file(hello_file)

if __name__ == '__main__':
    main()
