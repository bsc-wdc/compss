#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.ompss import ompss
from pycompss.api.constraint import constraint


@constraint(computing_units="1")
@ompss(binary="date")
@task()
def myDateConstrained(dprefix, param):
    pass


def main():
    from pycompss.api.api import compss_barrier
    myDateConstrained("-d", "next monday")
    compss_barrier()
    print("Finished")


if __name__ == '__main__':
    main()
