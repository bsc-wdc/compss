#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Barrier test
=========================
    This file represents PyCOMPSs Testbench.
    Checks the barrier API call
"""

# Imports
import time

from pycompss.api.api import compss_barrier

from tasks import get_hero


def launch_tasks(num_tasks):
    results = []
    for _ in range(num_tasks):
        r = get_hero()
        results.append(r)

    return results


def main_program():
    num_tasks = 20

    print ("Launching set of tasks with barrier")
    results = launch_tasks(num_tasks)

    start_time = time.time()
    compss_barrier()
    end_time = time.time()
    et1 = end_time - start_time

    print ("Launching set of tasks with barrier and noMoreTasks false")
    results = launch_tasks(num_tasks)

    start_time = time.time()
    compss_barrier(False)
    end_time = time.time()
    et2 = end_time - start_time

    print ("Launching set of tasks with barrier and noMoreTasks true")
    results = launch_tasks(num_tasks)

    start_time = time.time()
    compss_barrier(True)
    end_time = time.time()
    et3 = end_time - start_time

    print ("Results:")
    if et1 >= 1:
        print("- Test barrier(): OK")
    else:
        print("- Test barrier(): ERROR")

    if et2 >= 1:
        print("- Test barrier(false): OK")
    else:
        print("- Test barrier(false): ERROR")

    if et3 >= 1:
        print("- Test barrier(true): OK")
    else:
        print("- Test barrier(true): ERROR")


if __name__ == "__main__":
    main_program()
