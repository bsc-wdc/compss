#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs wait_on_generator test
================================
    Checks the compss_wait_on_generator API call.
    Launches 3 tasks sleeping 8, 5 and 1 seconds.
    Results must be yielded in completion order: 1, 5, 8.
"""

from pycompss.api.api import compss_wait_on_generator

from tasks import sleep_task


def main():
    f1 = sleep_task(8)
    f2 = sleep_task(5)
    f3 = sleep_task(1)

    results = list(compss_wait_on_generator([f1, f2, f3]))

    expected = [1, 5, 8]
    if results == expected:
        print("compss_wait_on_generator yielded results in completion order {}: OK".format(results))
    else:
        print("ERROR: compss_wait_on_generator returned {} instead of {}".format(results, expected))

    print("------------------------------------------------------------")


if __name__ == "__main__":
    main()
