#!/usr/bin/python

# -*- coding: utf-8 -*-
from pycompss.api.api import compss_delete_file, compss_file_exists, compss_open, compss_wait_on, compss_wait_on_file, compss_open, TaskGroup
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.constraint import constraint

import os

@task(returns=1)
def check_worker_environment():
    return os.environ["TEST_ENVIRONMENT"]


def main(): 
    print("Getting master environment: " + os.environ["TEST_ENVIRONMENT"])
    result = check_worker_environment()
    result = compss_wait_on(result)
    if result == "OK" :
        print("Getting worker environment: " + result)
    else:
        raise Exception(" Worker environment is incorrect" + str(result))

if __name__ == '__main__':
    main()
