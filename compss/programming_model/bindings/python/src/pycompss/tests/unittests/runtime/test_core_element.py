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

from pycompss.runtime.task.definitions.core_element import CE

ERROR_SIGNATURE = "ERROR: Wrong signature value."
ERROR_IMPL_SIGNATURE = "ERROR: Wrong impl_signature value."
ERROR_IMPL_CONSTRAINTS = "ERROR: Wrong impl_constraints value."
ERROR_IMPL_TYPE = "ERROR: Wrong impl_type value."
ERROR_IMPL_LOCAL = "ERROR: Wrong impl_local value."
ERROR_IMPL_IO = "ERROR: Wrong impl_io value."
ERROR_IMPL_PROLOG = "ERROR: Wrong impl_prolog value."
ERROR_IMPL_EPILOG = "ERROR: Wrong impl_epilog value."
ERROR_IMPL_CONTAINER = "ERROR: Wrong impl_container value."
ERROR_IMPL_TYPE_ARGS = "ERROR: Wrong impl_type_args value."


def test_core_element():
    signature = "my_signature"
    impl_signature = "my_impl_signature"
    impl_constraints = {"impl_constraints": ""}
    impl_type = "impl_type"
    impl_local = False
    impl_io = False
    impl_prolog = ["impl_prolog"]
    impl_epilog = ["impl_epilog"]
    impl_container = ["", "", ""]
    impl_type_args = ["impl_type_args"]
    core_element = CE(
        signature,
        impl_signature,
        impl_constraints,
        impl_type,
        impl_local,
        impl_io,
        impl_prolog,
        impl_epilog,
        impl_container,
        impl_type_args,
    )

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

    # Check impl_local
    result = core_element.get_impl_local()
    assert result == impl_local, ERROR_IMPL_LOCAL
    new_impl_local = True
    core_element.set_impl_local(new_impl_local)
    result = core_element.get_impl_local()
    assert result == new_impl_local, ERROR_IMPL_LOCAL

    # Check impl_io
    result = core_element.get_impl_io()
    assert result == impl_io, ERROR_IMPL_IO
    new_impl_io = True
    core_element.set_impl_io(new_impl_io)
    result = core_element.get_impl_io()
    assert result == new_impl_io, ERROR_IMPL_IO

    # Check impl_prolog
    result = core_element.get_impl_prolog()
    assert result == impl_prolog, ERROR_IMPL_PROLOG
    new_impl_prolog = ["my_new_impl_prolog"]
    core_element.set_impl_prolog(new_impl_prolog)
    result = core_element.get_impl_prolog()
    assert result == new_impl_prolog, ERROR_IMPL_PROLOG

    # Check impl_epilog
    result = core_element.get_impl_epilog()
    assert result == impl_epilog, ERROR_IMPL_EPILOG
    new_impl_epilog = ["my_new_impl_epilog"]
    core_element.set_impl_epilog(new_impl_epilog)
    result = core_element.get_impl_epilog()
    assert result == new_impl_epilog, ERROR_IMPL_EPILOG

    # Check impl_container
    result = core_element.get_impl_container()
    assert result == impl_container, ERROR_IMPL_CONTAINER
    new_impl_container = ["my_new_impl_container", "", ""]
    core_element.set_impl_container(new_impl_container)
    result = core_element.get_impl_container()
    assert result == new_impl_container, ERROR_IMPL_CONTAINER

    # Check impl_type_args
    result = core_element.get_impl_type_args()
    assert result == impl_type_args, ERROR_IMPL_TYPE_ARGS
    new_impl_type_args = ["my_new_impl_type_args"]
    core_element.set_impl_type_args(new_impl_type_args)
    result = core_element.get_impl_type_args()
    assert result == new_impl_type_args, ERROR_IMPL_TYPE_ARGS

    # Check representation
    representation = core_element.__repr__()
    assert isinstance(
        representation, str
    ), "ERROR: Received wrong representation type."  # noqa: E501
    expected = (
        "CORE ELEMENT: \n"
        "\t - CE signature     : my_new_signature\n"
        "\t - Impl. signature  : my_new_impl_signature\n"
        "\t - Impl. constraints: my_new_impl_constraints:value;\n"
        "\t - Impl. type       : my_new_impl_type\n"
        "\t - Impl. local      : True\n"
        "\t - Impl. io         : True\n"
        "\t - Impl. prolog     : ['my_new_impl_prolog']\n"
        "\t - Impl. epilog     : ['my_new_impl_epilog']\n"
        "\t - Impl. container  : ['my_new_impl_container', '', '']\n"
        "\t - Impl. type args  : ['my_new_impl_type_args']\n"
    )
    assert representation == expected, "ERROR: Wrong representation."

    # Reset
    core_element.reset()

    # Check again representation
    representation = core_element.__repr__()
    assert isinstance(
        representation, str
    ), "ERROR: Received wrong representation type."  # noqa: E501
    expected = (
        "CORE ELEMENT: \n"
        "\t - CE signature     : \n"
        "\t - Impl. signature  : \n"
        "\t - Impl. constraints: {}\n"
        "\t - Impl. type       : \n"
        "\t - Impl. local      : False\n"
        "\t - Impl. io         : False\n"
        "\t - Impl. prolog     : []\n"
        "\t - Impl. epilog     : []\n"
        "\t - Impl. container  : []\n"
        "\t - Impl. type args  : []\n"
    )
    assert representation == expected, "ERROR: Wrong empty representation."
