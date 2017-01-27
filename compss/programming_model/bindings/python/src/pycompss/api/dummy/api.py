#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: fconejer

PyCOMPSs Dummy API
==================
    This file defines the public PyCOMPSs API functions without functionality.
    It implements a dummy compss_open and compss_wait_on functions.
"""


def compss_open(obj):
    """
    Dummy compss_open
    :param obj: The object to open
    :return: The same object defined as parameter
    """
    return obj


def compss_delete(file_name):
    """
    Dummy compss_delete
    :param file_name: File name.
    """
    pass
  

def waitForAllTasks():
    """
    Dummy wait_for_all_tasks
    """
    pass
  

def compss_wait_on(obj):
    """
    Dummy compss_wait_on
    :param obj: The object to wait on.
    :return: The same object defined as parameter
    """
    return obj