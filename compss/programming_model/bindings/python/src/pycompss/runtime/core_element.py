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
PyCOMPSs Core Element
=====================
    This file contains the Core Element class, needed for the task registration.
"""


class CE(object):

    def __init__(self, ce_signature, impl_signature, impl_constraints, impl_type, impl_type_args):
        self.__ceSignature = ce_signature
        self.__implSignature = impl_signature
        self.__implConstraints = impl_constraints
        self.__implType = impl_type
        self.__implTypeArgs = impl_type_args

    ###########
    # GETTERS #
    ###########

    def get_ce_signature(self):
        return self.__ceSignature

    def get_impl_signature(self):
        return self.__implSignature

    def get_impl_constraints(self):
        return self.__implConstraints

    def get_impl_type(self):
        return self.__implType

    def get_impl_type_args(self):
        return self.__implTypeArgs

    ###########
    # SETTERS #
    ###########

    def set_ce_signature(self, ce_signature):
        self.__ceSignature = ce_signature

    def set_impl_signature(self, impl_signature):
        self.__implSignature = impl_signature

    def set_impl_constraints(self, impl_constraints):
        self.__implConstraints = impl_constraints

    def set_impl_type(self, impl_type):
        self.__implType = impl_type

    def set_impl_type_args(self, impl_type_args):
        self.__implTypeArgs = impl_type_args

    ##################
    # REPRESENTATION #
    ##################

    def __repr__(self):
        _repr = 'CORE ELEMENT: \n'
        _repr += '\t - CE signature     : ' + self.__ceSignature + '\n'
        _repr += '\t - Impl. signature  : ' + self.__implSignature + '\n'
        impl_constraints = ''
        for key, value in self.__implConstraints.items():
            impl_constraints += key + ':' + str(value) + ';'
        _repr += '\t - Impl. constraints: ' + impl_constraints + '\n'
        _repr += '\t - Impl. type       : ' + self.__implType + '\n'
        _repr += '\t - Impl. type args  : ' + ' '.join(self.__implTypeArgs)
        return _repr
