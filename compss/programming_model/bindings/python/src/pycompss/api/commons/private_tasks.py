#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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


@task(returns=1)
def transform(data, function, **kwargs):
    """Replace the user function with its @task equivalent.

    NOTE: Used from @data_transformation.

    :param data: the parameter that DT will be applied to.
    :param function: DT function
    :param kwargs: kwargs of the DT function
    :return:
    """
    return function(data, **kwargs)
