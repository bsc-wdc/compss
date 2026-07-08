#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import os
import shutil
import time
import uuid
from pycompss.api.api import compss_stop
from pycompss.api.parameter import FILE_INOUT, FILE_IN, FILE_OUT
from pycompss.api.task import task
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 6
STORAGE_PATH = "/tmp/sharedDisk/"
# Out-of-band marker files (not task parameters) shared between the workers
# and the master through the local filesystem, used to observe task progress
MARKER_PATH = STORAGE_PATH + "cancelAppTasksMarkers/"
# Only needs to outlive the stop propagation latency, not worker startup
TASK_DURATION = 4


@task(file_path=FILE_IN)
def long_task(file_path):
    marker_id = uuid.uuid4().hex
    open(MARKER_PATH + "started_" + marker_id, 'w').close()
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)
    time.sleep(TASK_DURATION)
    open(MARKER_PATH + "done_" + marker_id, 'w').close()


def count_markers(prefix):
    return len([f for f in os.listdir(MARKER_PATH) if f.startswith(prefix)])


def wait_until(condition, timeout=60, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise Exception("Timed out waiting for condition")


def create_file(file_name):
    # Clean previous ocurrences of the file
    if os.path.exists(file_name):
        os.remove(file_name)
    # Create file
    if not os.path.exists(STORAGE_PATH):
        os.mkdir(STORAGE_PATH)
    open(file_name, 'w').close()
    # Reset the marker directory
    if os.path.exists(MARKER_PATH):
        shutil.rmtree(MARKER_PATH)
    os.mkdir(MARKER_PATH)


def test_cancellation(file_name):
    for i in range (50):
        long_task(file_name)
    # Stop when the first wave (2 CUs) has finished and the second wave has
    # started but is still running, so the stop produces exactly 2 FINISHED
    # and 48 CANCELED tasks with jobs 1-4 only
    wait_until(lambda: count_markers("done_") == 2
               and count_markers("started_") == 4)

    compss_stop(1)
    #exit(1)


def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)

    print("[LOG] Test CANCEL APPLICATION TASKS")
    test_cancellation(file_name1)

if __name__ == '__main__':
    main()
