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

PyCOMPSs Core Element
=====================
    This file contains the Core Element class, needed for the task registration.
"""

# QUESTION: why is this a class instead of a named tuple?
#   from collections import namedtuple
#   CE = namedtuple('CE', 'ce_signature implSignature implConstraints implType implTypeArgs')
# QUESTION: why there are getters and setters if all attributes are "public"?
class CE:
    ce_signature = ""
    implSignature = ""
    implConstraints = {}
    implType = ""
    implTypeArgs = []

    def __init__(self, ce_signature, impl_signature, impl_constraints, impl_type, impl_typeArgs):
        self.ce_signature = ce_signature
        self.implSignature = impl_signature
        self.implConstraints = impl_constraints
        self.implType = impl_type
        self.implTypeArgs = impl_typeArgs

    # GETTERS

    def get_ce_signature(self):
        return self.ce_signature

    def get_implSignature(self):
        return self.implSignature

    def get_implConstraints(self):
        return self.implConstraints

    def get_implType(self):
        return self.implType

    def get_implTypeArgs(self):
        return self.implTypeArgs

    # SETTERS

    def set_ce_signature(self, ce_signature):
        self.ce_signature = ce_signature

    def set_implSignature(self, impl_signature):
        self.implSignature = impl_signature

    def set_implConstraints(self, impl_constraints):
        self.implConstraints = impl_constraints

    def set_implType(self, impl_type):
        self.implType = impl_type

    def set_implTypeArgs(self, impl_typeArgs):
        self.implTypeArgs = impl_typeArgs

    # Representation
    def __repr__(self):
        repr = 'CORE ELEMENT: \n'
        repr += '\t - CE signature    : ' + self.ce_signature + '\n'
        repr += '\t - Impl. signature : ' + self.implSignature + '\n'
        implConstraints = ''
        for key, value in self.implConstraints.iteritems():
            implConstraints += key + ":" + str(value) + ";"
        repr += '\t - Impl. constrings: ' + implConstraints + '\n'
        repr += '\t - Impl. type      : ' + self.implType + '\n'
        repr += '\t - Impl. type args : ' + ' '.join(self.implTypeArgs)
        return repr

