#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs wait_on_any test
=========================
    Checks the compss_wait_on_any API call.
    Launches 3 tasks sleeping 8, 5 and 1 seconds.
    The first available result must come from the 1-second task.
"""

from pycompss.api.api import compss_wait_on_any

from tasks import sleep_task


def main():
    f1 = sleep_task(8)
    f2 = sleep_task(5)
    f3 = sleep_task(1)

    result = compss_wait_on_any([f1, f2, f3])

    if result == 1:
        print("compss_wait_on_any returned the first completed task (1s): OK")
    else:
        print("ERROR: compss_wait_on_any returned {} instead of 1".format(result))

    print("------------------------------------------------------------")


if __name__ == "__main__":
    main()
