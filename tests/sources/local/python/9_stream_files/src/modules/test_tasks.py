#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
import os
import uuid
import time

from pycompss.api.task import task
from pycompss.api.parameter import *

# Constant values
NUM_FILES = 10
TEST_PATH = "/tmp/file_stream_python/"
BASE_FILE_NAME = "file"


# Task definitions


@task(fds=STREAM_OUT, sleep=IN)
def write_files(fds, sleep):
    for i in range(NUM_FILES):
        # Build file name
        file_name = BASE_FILE_NAME + str(uuid.uuid4())
        file_path = os.path.join(TEST_PATH, file_name)

        # Write file
        print("Writing file: " + str(file_path))
        with open(file_path, 'w') as f:
            f.write("Test " + str(i))

        # Sleep between generated files
        time.sleep(sleep)

    # Mark the stream for closure
    fds.close()


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


@task(file_path=FILE_IN, returns=int)
def process_file(file_path):
    print("RECEIVED FILE: " + str(file_path))
    with open(file_path, 'r') as f:
        print (f.read())

    return 1
