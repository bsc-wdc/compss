#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

# -*- coding: utf-8 -*-

"""
PyCOMPSs Dummy API
==================
    This file defines the public PyCOMPSs API functions without functionality.
    It implements a dummy compss_open and compss_wait_on functions.
"""


def compss_start():
    """
    Dummy runtime starter.

    :return: None
    """

    pass


def compss_stop():
    """
    Dummy runtime stopper.

    :return: None
    """

    pass


def compss_open(file_name, mode='r'):
    """
    Dummy compss_open.

    :param file_name: The file name to open
    :return: An object of 'file' type.
    :raise IOError: If the file can not be opened.
    """

    return open(file_name, mode)


def compss_delete_file(file_name):
    """
    Dummy compss_delete.

    :param file_name: File name.
    :return: Always True.
    """

    return True


def compss_delete_object(obj):
    """
    Dummy compss_delete_object.

    :param obj: Object.
    :return: Always True.
    """

    return True


def compss_barrier(no_more_tasks=False):
    """
    Dummy barrier.

    :param no_more_tasks: No more tasks boolean
    :return: None
    """
    pass


def compss_wait_on(*args):
    """
    Dummy compss_wait_on

    :param args: Objects to wait on
    :return: The same objects defined as parameter
    """

    ret = list(map(lambda o: o, args))
    ret = ret[0] if len(ret) == 1 else ret
    return ret
