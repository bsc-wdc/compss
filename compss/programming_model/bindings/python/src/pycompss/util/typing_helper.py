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
PyCOMPSs Utils - typing_helper.

This file contains the typing helpers.
"""

import typing


class dummy_mypyc_attr(object):
    """Dummy on mypy_attr class (decorator style)."""

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Create a new dummy mypy attribute.

        :returns: None.
        """
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f: typing.Any) -> typing.Any:
        """Execute the given function.

        :param f: Decorated function.
        :returns: Decorated function execution result.
        """
        def wrapped_mypyc_attr(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return f(*args, **kwargs)

        return wrapped_mypyc_attr


import_ok = True
try:
    from mypy_extensions import mypyc_attr as real_mypyc_attr

    # https://mypyc.readthedocs.io/en/latest/native_classes.html#inheritance
except ImportError:
    # Dummy mypyc_attr just in case mypy_extensions is not installed
    import_ok = False

if import_ok:
    mypyc_attr = real_mypyc_attr
else:
    mypyc_attr = dummy_mypyc_attr


######################################
# Boilerplate to mimic user fuctions #
######################################


def dummy_function() -> None:
    """Do nothing function.

    :returns: None
    """
    pass
