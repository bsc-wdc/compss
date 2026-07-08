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
from pycompss.api.api import compss_barrier_group, compss_cancel_group, TaskGroup
from pycompss.api.parameter import FILE_INOUT, FILE_IN
from pycompss.api.task import task
from pycompss.api.exceptions import COMPSsException

NUM_TASKS = 3
NUM_GROUPS = 3
STORAGE_PATH = "/tmp/sharedDisk/"
# Out-of-band marker files (not task parameters) shared between the workers
# and the master through the local filesystem, used to observe task progress
MARKER_PATH = STORAGE_PATH + "cancelTaskGroupsMarkers/"
# Only needs to outlive the cancel propagation latency, not worker startup
TASK_DURATION = 4

@task(file_path=FILE_INOUT)
def write_one(file_path):
    marker_id = uuid.uuid4().hex
    open(MARKER_PATH + "started_" + marker_id, 'w').close()
    time.sleep(TASK_DURATION)
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(1)
        fos.write(new_value)
    open(MARKER_PATH + "done_" + marker_id, 'w').close()


@task(file_path=FILE_INOUT)
def write_two(file_path):
    # Write value
    with open(file_path, 'a') as fos:
        new_value = str(2)
        fos.write(new_value)


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


def test_task_groups(file_name):

    with TaskGroup('bigGroup', False):
        # Inside a big group, more groups are created
        for i in range(NUM_GROUPS):
            with(TaskGroup('group'+str(i), False)):
                for j in range(NUM_TASKS):
                     write_one(file_name)
    # The write_one tasks serialize on the FILE_INOUT chain. Cancel when the
    # first one has finished and the second one has started but is still
    # running, so the file ends up with exactly one '1' (expected: 1222)
    wait_until(lambda: count_markers("done_") == 1
               and count_markers("started_") == 2)
    # Barrier for groups
    for i in range(NUM_GROUPS):
         compss_cancel_group('group'+str(i))

    # Creation of group
    with TaskGroup('individualGroup', True):
        for i in range(NUM_TASKS):
            write_two(file_name)

def main():
    file_name1 = STORAGE_PATH + "taskGROUPS.txt"
    create_file(file_name1)
    print ("[LOG] Test TASK GROUPS implicit barrier")
    test_task_groups(file_name1)

if __name__ == '__main__':
    main()
