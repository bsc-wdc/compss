#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs API - COMMONS - PRIVATE TASKS.

This file contains tasks required by any decorator.

WARNING: This file can not be compiled with mypy since it contains
         @task decorated functions.
"""

from pycompss.api.task import task
from pycompss.api.parameter import COLLECTION_IN
from pycompss.api.parameter import FILE_OUT
from pycompss.api.parameter import FILE_IN
from pycompss.api.parameter import DIRECTORY_IN
from pycompss.api.parameter import DIRECTORY_OUT


@task(returns=1)
def transform(target, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to.
    @param function: DT function
    @param kwargs: kwargs of the DT function
    :return:
    """
    return function(target, **kwargs)


@task(returns=object, target=COLLECTION_IN)
def col_to_obj(target, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param function: DT function which accepts a collection as input and
    produces an object.
    :return:
    """
    return function(target, **kwargs)


@task(destination=FILE_OUT, target=COLLECTION_IN)
def col_to_file(target, destination, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param destination: name of the file that will be produced by this task
    @param function: DT function which accepts a collection as input and
    produces a file.
    :return:
    """
    function(target, destination, **kwargs)


@task(destination=DIRECTORY_OUT, target=COLLECTION_IN)
def col_to_dir(target, destination, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param destination: name of the file that will be produced by this task
    @param function: DT function which accepts a collection as input and
    produces a file.
    :return:
    """
    function(target, destination, **kwargs)


@task(returns=object(), target=FILE_IN)
def file_to_object(target, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param function: DT function which accepts a file as input and
    produces an object.
    :return:
    """
    return function(target, **kwargs)


@task(returns=object(), target=DIRECTORY_IN)
def dir_to_object(target, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param function: DT function which accepts a file as input and
    produces an object.
    :return:
    """
    return function(target, **kwargs)


@task(destination=FILE_OUT)
def object_to_file(target, destination, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param destination: name of the file that will be produced by this task
    @param function: DT function which accepts an object as input and
    produces a file.
    :return:
    """
    function(target, destination, **kwargs)


@task(destination=DIRECTORY_OUT)
def object_to_dir(target, destination, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param destination: name of the file that will be produced by this task
    @param function: DT function which accepts an object as input and
    produces a file.
    :return:
    """
    function(target, destination, **kwargs)


@task(target=FILE_IN)
def file_to_col(target, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    @param target: the parameter that DT will be applied to
    @param function: DT function which accepts a file as input and
    produces a collection.
    :return:
    """
    return function(target, **kwargs)


# @task(target=FILE_IN, destination=COLLECTION_OUT)
# def _file_to_col(target, function, destination):
#     function(target)
