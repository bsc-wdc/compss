#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.api import compss_wait_on
from pycompss.api.task import task
from pycompss.api.on_failure import on_failure


@on_failure(management="CANCEL_SUCCESSORS")
@task(returns=1)
def generate_data():
    obj = [1, 2, 3, 4]
    raise Exception("ON PURPOSE EXCEPTION TO TRIGGER CANCEL SUCCESSORS")
    return obj


@task(returns=1)
def accumulate(obj):
    result = 0
    for i in obj:
        result = result + i
    return result


def test_wait_on_cancelled_task():
    # Task that triggers an exception:
    obj = generate_data()
    # Task that is going to be cancelled:
    result = accumulate(obj)
    # Wait for the result (not produced by accumulate since it has been cancelled)
    result = compss_wait_on(result)

    if result != None:
        print("ERROR: Expected to receive None, but received: " + str(result))
    else:
        print("OK: Received None from cancelled task return.")


def main():
    print("[LOG] Test WAIT ON CANCELLED TASK")
    test_wait_on_cancelled_task()


if __name__ == '__main__':
    main()
