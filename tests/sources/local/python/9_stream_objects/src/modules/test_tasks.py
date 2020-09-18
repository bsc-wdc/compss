#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
import uuid
import time

from pycompss.api.task import task
from pycompss.api.parameter import *

from modules.models import MyObject

# Constant values
NUM_OBJECTS = 10


# Task definitions

@task(ods=STREAM_OUT, sleep=IN)
def write_objects(ods, sleep):
    for i in range(NUM_OBJECTS):
        # Build object
        obj = MyObject(name=str(uuid.uuid4()), age=i)

        # Publish object
        ods.publish(obj)

        # Sleep between generated files
        time.sleep(sleep)

    # Mark the stream for closure
    ods.close()


@task(ods=STREAM_IN, sleep=IN, returns=int)
def read_objects(ods, sleep):
    num_total = 0
    while not ods.is_closed():
        # Poll new objects
        print("Polling objects")
        new_objects = ods.poll()

        # Process files
        for obj in new_objects:
            print("RECEIVED OBJECT: " + str(obj))

        # Accumulate read files
        num_total = num_total + len(new_objects)

        # Sleep between requests
        time.sleep(sleep)

    # Read one last time
    print("Polling objects")
    new_objects = ods.poll()
    for obj in new_objects:
        print("RECEIVED OBJECT: " + str(obj))
    num_total = num_total + len(new_objects)

    # Return the number of processed files
    return num_total


@task(file_path=IN, returns=int)
def process_object(obj):
    print("RECEIVED OBJECT: " + str(obj))
    return 1
