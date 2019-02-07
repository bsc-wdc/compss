#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
from models import MyFile

@task(targetDirection='CONCURRENT')
def write_one(file):
    print("Init task user code")

    # Write value
    file.writeOne()

if __name__ == '__main__':

    path = "/tmp/sharedDisk/file.txt"

    open(path, 'w').close()

    file=MyFile(path)

    file.makePersistent()

    for i in range(4):
        # Execute increment
        write_one(file)
        count = file.countOnes()
        # with open(path, 'a') as fos:
        #     new_value = "1"
        #     fos.write(new_value)
        #
        # with compss_open(path, 'r') as fis:
        #     final_value = fis.read()
        # total = final_value.count('1')
        print("path of file is ", file.getPath())
        print (i, " wrote one and there were ", count, " ones written")


    total = file.countOnes()
    print("Final counter value is " + str(total) + " ones")

    file.deletePersistent()

    if file.get() == "/tmp/sharedDisk/file.txt":
        print("- Persitence of PSCOS in task as return: OK")
    else:
      print("- Persitence of PSCOS in task as return: ERROR")