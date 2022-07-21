#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import time
from pycompss.api.api import compss_open
from pycompss.api.api import compss_wait_on_file
from pycompss.api.task import task
from pycompss.api.parameter import *

@task(out_coll=COLLECTION_FILE_OUT)
def gen_collection(out_coll):
    for out_file in out_coll:
       text_file = open(out_file, "w")
       text_file.write(str(1))
       text_file.close()

@task(inout_coll=COLLECTION_FILE_INOUT)
def update_collection(inout_coll):
    for inout_file in inout_coll:
        print("Writing one in file " + inout_file)
        text_file = open(inout_file, "a")
        text_file.write(str(1))
        text_file.close()

@task(in_coll=COLLECTION_FILE_IN)
def read_collection(in_coll):
    for in_file in in_coll:
       text_file = open(in_file, "r")
       val = text_file.readline()
       text_file.close()
       #if int(val) == 11:
       #    print("Value correct")
       #else:
       #    raise Exception("Incorrect value read " + val)


def depth_one():
    file_collection = ['file_1', 'file_2', 'file_3']
    gen_collection(file_collection)
    update_collection(file_collection)
    read_collection(file_collection)
    read_collection(file_collection)
    for in_file in file_collection:
       text_file = compss_open(in_file, "r")
       val = text_file.readline()
       text_file.close()
       if int(val) == 11:
           print("Value correct for " + in_file)
       else:
           raise Exception("Incorrect value for " + in_file + "Value was " + val + " (Expecting 11)" )


@task(out_coll=COLLECTION_FILE_OUT)
def gen_collection_two(out_coll):
    for row in out_coll:
        if isinstance(row, list):
            for out_file in row:
                text_file = open(out_file, "w")
                text_file.write(str(1))
                text_file.close()
        else:
            text_file = open(row, "w")
            text_file.write(str(1))
            text_file.close()

@task(inout_coll=COLLECTION_FILE_INOUT)
def update_collection_two(inout_coll):
    for row in inout_coll:
        if isinstance(row, list):
            for inout_file in row:
                print("Writing one in file " + inout_file)
                text_file = open(inout_file, "a")
                text_file.write(str(1))
                text_file.close()
        else:
            print("Writing one in file " + row)
            text_file = open(row, "a")
            text_file.write(str(1))
            text_file.close()

@task(in_coll=COLLECTION_FILE_IN)
def read_collection_two(in_coll):
    for row in in_coll:
        if isinstance(row, list):
            for in_file in row:
               text_file = open(in_file, "r")
               val = text_file.readline()
               text_file.close()
               #if int(val) == 11:
               #    print("Value correct")
               #else:
               #    raise Exception("Incorrect value read " + val)
        else:
            text_file = open(row, "r")
            val = text_file.readline()
            text_file.close()
            #if int(val) == 11:
            #    print("Value correct")
            #else:
            #    raise Exception("Incorrect value read " + val)


def depth_two():
    file_collection = [['file_1', 'file_2'], ['file_3', 'file_4'], 'file_5']
    gen_collection_two(file_collection)
    update_collection_two(file_collection)
    read_collection_two(file_collection)
    read_collection_two(file_collection)
    for row in file_collection:
        if isinstance(row, list):
            for in_file in row:
               text_file = compss_open(in_file, "r")
               val = text_file.readline()
               text_file.close()
               if int(val) == 11:
                   print("Value correct for " + in_file)
               else:
                   raise Exception("Incorrect value for " + in_file + "Value was " + val + " (Expecting 11)" )
        else:
            text_file = compss_open(row, "r")
            val = text_file.readline()
            text_file.close()
            if int(val) == 11:
                print("Value correct for " + row)
            else:
                raise Exception("Incorrect value for " + row + "Value was " + val + " (Expecting 11)" )


def relative_path():
    file_collection = ['file_1', 'file_2', 'file_3']
    gen_collection(file_collection)
    for f in file_collection:
        compss_wait_on_file(f)


def main():
    depth_one()
    depth_two()
    relative_path()


if __name__ == '__main__':
    main()
