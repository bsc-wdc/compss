#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
import time

from pycompss.api.task import task
from pycompss.api.binary import binary
from pycompss.api.parameter import *

# Constant values
TEST_PATH = "/tmp/non_native_stream_python/"


# Task definitions


@binary(binary="${WRITE_BINARY}")
@task(fds=STREAM_OUT, sleep=IN)
def write_files(fds, sleep):
    pass


@task(fds=STREAM_IN, sleep=IN, returns=int)
def read_files(fds, sleep):
    num_total = 0
    while not fds.is_closed():
        # Poll new files
        print("Polling files")
        new_files = fds.poll()

        # Process files
        for nf in new_files:
            print("RECEIVED FILE: " + str(nf))
            with open(nf, 'r') as f:
                print (f.read())

        # Accumulate read files
        num_total = num_total + len(new_files)

        # Sleep between requests
        time.sleep(sleep)

    # Return the number of processed files
    return num_total
