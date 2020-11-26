#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_wait_on_file
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

import time
import random
import os
from os import path

@task(structure=FILE_OUT)
def fix_side_chain(i, structure=None, **kwargs):
    if (structure is not None):
        if i != 1 :
            raise Exception("Exception shouldn't be different from None")
        f = open(structure, "w")
        f.write("Writting structure")
        f.close()
    elif i == 1:
        raise Exception("Exception shouldn't be none")
    time.sleep(2.0+(float(random.randint(20,30))/100.0))

@task(structure=FILE_OUT)
def fix_side_chain_2(i, structure, **kwargs):
    if (structure is not None):
        if i != 1 :
            raise Exception("Exception shouldn't be different from None")
        f = open(structure, "w")
        f.write("Writting structure")
        f.close()
    elif i == 1:
        raise Exception("Exception shouldn't be none")
    time.sleep(2.0+(float(random.randint(20,30))/100.0))

@task(structure=FILE_OUT)
def fix_side_chain_3(i, structure="hola", **kwargs):
    if (structure is not None):
        if structure == "hola" and (i != 0 or i != 3):
            raise Exception("Exception shouldn't be different from hola")
        f = open(structure, "w")
        f.write("Writting " + structure)
        f.close()
    elif i != 2:
        raise Exception("Exception shouldn't be none")
    time.sleep(2.0+(float(random.randint(20,30))/100.0))

def main():
    st = "structure_1"
    fix_side_chain(0)
    fix_side_chain(1, structure=st)
    fix_side_chain(2)
    compss_wait_on_file(st)
    st = "structure_2"
    fix_side_chain_2(0,None)
    fix_side_chain_2(1,st)
    fix_side_chain_2(2,None)
    compss_wait_on_file(st)
    st = "structure_3"
    fix_side_chain_3(0)
    fix_side_chain_3(1,structure=st)
    fix_side_chain_3(2,None)
    fix_side_chain_3(3)
    compss_wait_on_file("hola")
    compss_wait_on_file(st)
if __name__ == '__main__':
    main()
