#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

from pycompss.runtime.task.core_element import CE

ERROR_SIGNATURE = "ERROR: Wrong signature value."
ERROR_IMPL_SIGNATURE = "ERROR: Wrong impl_signature value."
ERROR_IMPL_CONSTRAINTS = "ERROR: Wrong impl_constraints value."
ERROR_IMPL_TYPE = "ERROR: Wrong impl_type value."
ERROR_IMPL_IO = "ERROR: Wrong impl_io value."
ERROR_IMPL_TYPE_ARGS = "ERROR: Wrong impl_type_args value."


def test_core_element():
    signature = "my_signature"
    impl_signature = "my_impl_signature"
    impl_constraints = "impl_constraints"
    impl_type = "impl_type"
    impl_io = "impl_io"
    impl_type_args = "impl_type_args"
    core_element = CE(signature,
                      impl_signature,
                      impl_constraints,
                      impl_type,
                      impl_io,
                      impl_type_args)

    # Check signature
    result = core_element.get_ce_signature()
    assert result == signature, ERROR_SIGNATURE
    new_signature = "my_new_signature"
    core_element.set_ce_signature(new_signature)
    result = core_element.get_ce_signature()
    assert result == new_signature, ERROR_SIGNATURE

    # Check impl_signature
    result = core_element.get_impl_signature()
    assert result == impl_signature, ERROR_IMPL_SIGNATURE
    new_impl_signature = "my_new_impl_signature"
    core_element.set_impl_signature(new_impl_signature)
    result = core_element.get_impl_signature()
    assert result == new_impl_signature, ERROR_IMPL_SIGNATURE

    # Check impl_constraints
    result = core_element.get_impl_constraints()
    assert result == impl_constraints, ERROR_IMPL_CONSTRAINTS
    new_impl_constraints = {"my_new_impl_constraints": "value"}
    core_element.set_impl_constraints(new_impl_constraints)
    result = core_element.get_impl_constraints()
    assert result == new_impl_constraints, ERROR_IMPL_CONSTRAINTS

    # Check impl_type
    result = core_element.get_impl_type()
    assert result == impl_type, ERROR_IMPL_TYPE
    new_impl_type = "my_new_impl_type"
    core_element.set_impl_type(new_impl_type)
    result = core_element.get_impl_type()
    assert result == new_impl_type, ERROR_IMPL_TYPE

    # Check impl_io
    result = core_element.get_impl_io()
    assert result == impl_io, ERROR_IMPL_IO
    new_impl_io = "my_new_impl_io"
    core_element.set_impl_io(new_impl_io)
    result = core_element.get_impl_io()
    assert result == new_impl_io, ERROR_IMPL_IO

    # Check impl_type_args
    result = core_element.get_impl_type_args()
    assert result == impl_type_args, ERROR_IMPL_TYPE_ARGS
    new_impl_type_args = "my_new_impl_type_args"
    core_element.set_impl_type_args(new_impl_type_args)
    result = core_element.get_impl_type_args()
    assert result == new_impl_type_args, ERROR_IMPL_TYPE_ARGS

    # Check representation
    representation = core_element.__repr__()
    assert isinstance(representation, str), "ERROR: Received wrong representation type."  # noqa: E501
    expected = "CORE ELEMENT: \n" \
               "\t - CE signature     : my_new_signature\n" \
               "\t - Impl. signature  : my_new_impl_signature\n" \
               "\t - Impl. constraints: my_new_impl_constraints:value;\n" \
               "\t - Impl. type       : my_new_impl_type\n" \
               "\t - Impl. io         : my_new_impl_io\n" \
               "\t - Impl. type args  : my_new_impl_type_args"
    assert representation == expected, "ERROR: Wrong representation."

    # Reset
    core_element.reset()

    # Check again representation
    representation = core_element.__repr__()
    assert isinstance(representation, str), "ERROR: Received wrong representation type."  # noqa: E501
    expected = "CORE ELEMENT: \n" \
               "\t - CE signature     : None\n" \
               "\t - Impl. signature  : None\n" \
               "\t - Impl. constraints: None\n" \
               "\t - Impl. type       : None\n" \
               "\t - Impl. io         : None\n" \
               "\t - Impl. type args  : None"
    assert representation == expected, "ERROR: Wrong empty representation."
