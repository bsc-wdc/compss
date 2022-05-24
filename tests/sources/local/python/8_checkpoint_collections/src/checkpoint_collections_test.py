#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.exceptions import COMPSsException
from pycompss.api.api import compss_delete_file, compss_delete_object, compss_file_exists, compss_open, compss_wait_on, compss_wait_on_file, compss_open, TaskGroup
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint
from modules.shape import Shape

import time
import random
import os
from os import path

@task(mylist=COLLECTION_INOUT)
def scale_all(mylist, scale):
    for ll in mylist:
        ll.x = ll.x * scale
        ll.y = ll.y * scale


def main():
    if len(sys.argv) != 2:
        exit(-1)
    exception = sys.argv[1]

    my_shapes = []
    my_shapes.append(Shape(100,45))
    my_shapes.append(Shape(50,50))
    my_shapes.append(Shape(10,100))
    my_shapes.append(Shape(20,30))

    scale_all(my_shapes,2)
    scale_all(my_shapes,2)
    scale_all(my_shapes,2)
    if exception == "1":
        time.sleep(10)
        raise Exception("Error")
    scale_all(my_shapes,2)

    scaled_areas=[]
    for this_shape in my_shapes:
        scaled_areas.append(this_shape.area())

    scaled_areas = compss_wait_on(scaled_areas)
    print()
    print()
    print()
    print("PRINTING RESULT")
    print(scaled_areas)
    print()
    print()
    print()

'''
def main():
    # Check and get parameters
    if len(sys.argv) != 2:
        exit(-1)

    exception = sys.argv[1]

    fileName = "fileName"
    initialValue = "1"
    # Write value
    fos = open(fileName, 'w')
    fos.write(initialValue)
    fos.close()
    print("Initial counter value is " + str(initialValue))

    # Execute increment
    increment(fileName)
    increment(fileName)
    increment(fileName)

    fis = compss_open(fileName, 'r')
    middleValue = fis.read()
    fis.close()
    print("Middle counter value is " + str(middleValue))
    increment(fileName)


    increment(fileName)

    # Write new value
    # Falta que compss_open al acabar resti un al reader
    fis = compss_open(fileName, 'r')
    finalValue = fis.read()
    fis.close()
    print("Final counter value is " + str(finalValue))
'''

if __name__ == '__main__':
    main()
