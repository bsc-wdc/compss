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
PyCOMPSs Core Element
=====================
    This file contains the Core Element class, needed for the task
    registration.
"""

import typing


class CE(object):

    __slots__ = ["ce_signature", "impl_signature", "impl_constraints",
                 "impl_type", "impl_io", "impl_type_args", "empty"]

    def __init__(self,
                 ce_signature="",        # type: str
                 impl_signature="",      # type: str
                 impl_constraints=None,  # type: typing.Dict[str, str]
                 impl_type=None,         # type: typing.Optional[str]
                 impl_io=False,          # type: bool
                 impl_type_args=None,    # type: typing.List[str]
                 ):                      # type: (...) -> None
        self.ce_signature = ce_signature
        self.impl_signature = impl_signature
        if impl_constraints is None:
            self.impl_constraints = dict()
        else:
            self.impl_constraints = impl_constraints
        self.impl_type = impl_type
        self.impl_io = impl_io
        if impl_type_args is None:
            self.impl_type_args = list()
        else:
            self.impl_type_args = impl_type_args

    ###########
    # METHODS #
    ###########

    def reset(self):
        # type: () -> None
        self.ce_signature = ""
        self.impl_signature = ""
        self.impl_constraints = dict()
        self.impl_type = ""
        self.impl_io = False
        self.impl_type_args = list()

    ###########
    # GETTERS #
    ###########

    def get_ce_signature(self):
        # type: () -> str
        return self.ce_signature

    def get_impl_signature(self):
        # type: () -> str
        return self.impl_signature

    def get_impl_constraints(self):
        # type: () -> typing.Dict[str, str]
        return self.impl_constraints

    def get_impl_type(self):
        # type: () -> typing.Optional[str]
        return self.impl_type

    def get_impl_io(self):
        # type: () -> bool
        return self.impl_io

    def get_impl_type_args(self):
        # type: () -> typing.List[str]
        return self.impl_type_args

    ###########
    # SETTERS #
    ###########

    def set_ce_signature(self, ce_signature):
        # type: (str) -> None
        self.ce_signature = ce_signature

    def set_impl_signature(self, impl_signature):
        # type: (str) -> None
        self.impl_signature = impl_signature

    def set_impl_constraints(self, impl_constraints):
        # type: (dict) -> None
        self.impl_constraints = impl_constraints

    def set_impl_type(self, impl_type):
        # type: (str) -> None
        self.impl_type = impl_type

    def set_impl_io(self, impl_io):
        # type: (bool) -> None
        self.impl_io = impl_io

    def set_impl_type_args(self, impl_type_args):
        # type: (list) -> None
        self.impl_type_args = impl_type_args

    ##################
    # REPRESENTATION #
    ##################

    def __repr__(self):
        # type: () -> str
        """ Builds the element representation as string.

        :return: The core element representation.
        """
        _repr = "CORE ELEMENT: \n"
        _repr += "\t - CE signature     : " + str(self.ce_signature) + "\n"
        _repr += "\t - Impl. signature  : " + str(self.impl_signature) + "\n"
        if self.impl_constraints:
            impl_constraints = ""
            for key, value in self.impl_constraints.items():
                impl_constraints += key + ":" + str(value) + ";"
        else:
            impl_constraints = str(self.impl_constraints)
        _repr += "\t - Impl. constraints: " + impl_constraints + "\n"
        _repr += "\t - Impl. type       : " + str(self.impl_type) + "\n"
        _repr += "\t - Impl. io         : " + str(self.impl_io) + "\n"
        _repr += "\t - Impl. type args  : " + str(self.impl_type_args)
        return _repr
