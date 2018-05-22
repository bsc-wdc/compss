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


# QUESTION: why is this a class instead of a named tuple?
#   from collections import namedtuple
#   CE = namedtuple('CE', 'ce_signature implSignature implConstraints implType implTypeArgs')
# QUESTION: why there are getters and setters if all attributes are "public"?
class CE:

    def __init__(self, ce_signature, impl_signature, impl_constraints, impl_type, impl_type_args):
        self.ce_signature = ce_signature
        self.impl_signature = impl_signature
        self.impl_constraints = impl_constraints
        self.impl_type = impl_type
        self.impl_type_args = impl_type_args

    # GETTERS
    def get_ce_signature(self):
        return self.ce_signature

    def get_impl_signature(self):
        return self.impl_signature

    def get_impl_constraints(self):
        return self.impl_constraints

    def get_impl_type(self):
        return self.impl_type

    def get_impl_type_args(self):
        return self.impl_type_args

    # SETTERS
    def set_ce_signature(self, ce_signature):
        self.ce_signature = ce_signature

    def set_impl_signature(self, impl_signature):
        self.impl_signature = impl_signature

    def set_impl_constraints(self, impl_constraints):
        self.impl_constraints = impl_constraints

    def set_impl_type(self, impl_type):
        self.impl_type = impl_type

    def set_impl_type_args(self, impl_typeArgs):
        self.impl_type_args = impl_typeArgs

    # Representation
    def __repr__(self):
        repr = 'CORE ELEMENT: \n'
        repr += '\t - CE signature     : ' + self.ce_signature + '\n'
        repr += '\t - Impl. signature  : ' + self.impl_signature + '\n'
        impl_constraints = ''
        for key, value in self.impl_constraints.items():
            impl_constraints += key + ":" + str(value) + ";"
        repr += '\t - Impl. constraints: ' + impl_constraints + '\n'
        repr += '\t - Impl. type       : ' + self.impl_type + '\n'
        repr += '\t - Impl. type args  : ' + ' '.join(self.impl_type_args)

        return repr
