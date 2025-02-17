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
PyCOMPSs runtime - Task - Definitions - Core element.

This file contains the Core Element class, needed for the task registration.
"""

from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.util.typing_helper import typing


class CE:  # pylint: disable=too-many-instance-attributes
    """Core Element class."""

    __slots__ = [
        "ce_signature",
        "impl_signature",
        "impl_constraints",
        "impl_type",
        "impl_local",
        "impl_io",
        "impl_prolog",
        "impl_epilog",
        "impl_container",
        "impl_type_args",
    ]

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ce_signature: str = "",
        impl_signature: str = "",
        impl_constraints: typing.Optional[
            typing.Dict[str, typing.Union[str, int]]
        ] = None,
        impl_type: typing.Optional[str] = None,
        impl_local: bool = False,
        impl_io: bool = False,
        impl_prolog: typing.Optional[typing.List[str]] = None,
        impl_epilog: typing.Optional[typing.List[str]] = None,
        impl_container: typing.Optional[typing.List[str]] = None,
        impl_type_args: typing.Optional[typing.List[str]] = None,
    ) -> None:
        """Core Element constructor.

        :param ce_signature: Core element signature.
        :param impl_signature: Implementation signature.
        :param impl_constraints: Implementation constraints.
        :param impl_type: Implementation type.
        :param impl_local: If the implementation has to be executed locally.
        :param impl_io: If the implementation has IO requirements.
        :param impl_prolog: Implementation prolog.
        :param impl_epilog: Implementation epilog.
        :param impl_container: Implementation container.
        :param impl_type_args: Implementation type arguments.
        """
        self.ce_signature = ce_signature
        self.impl_signature = impl_signature
        if impl_constraints is None:
            self.impl_constraints = {}
        else:
            self.impl_constraints = impl_constraints
        self.impl_type = impl_type
        self.impl_local = impl_local
        self.impl_io = impl_io
        if impl_type_args is None:
            self.impl_type_args = []
        else:
            self.impl_type_args = impl_type_args

        self.impl_prolog = (
            [INTERNAL_LABELS.unassigned] * 3
            if impl_prolog is None
            else impl_prolog
        )
        self.impl_epilog = (
            [INTERNAL_LABELS.unassigned] * 3
            if impl_epilog is None
            else impl_epilog
        )
        # engine, image, options
        self.impl_container = (
            [INTERNAL_LABELS.unassigned] * 3
            if impl_container is None
            else impl_container
        )

    ###########
    # METHODS #
    ###########

    def reset(self) -> None:
        """Reset the core element.

        :returns: None.
        """
        self.ce_signature = ""
        self.impl_signature = ""
        self.impl_constraints = {}
        self.impl_type = ""
        self.impl_io = False
        self.impl_local = False
        self.impl_type_args = []
        self.impl_prolog = []
        self.impl_epilog = []
        self.impl_container = []

    ###########
    # GETTERS #
    ###########

    def get_ce_signature(self) -> str:
        """Get the core element signature.

        :return: The core element signature.
        """
        return self.ce_signature

    def get_impl_signature(self) -> str:
        """Get the core element implementation signature.

        :return: The core element implementation signature.
        """
        return self.impl_signature

    def get_impl_constraints(self) -> typing.Dict[str, typing.Union[str, int]]:
        """Get the core element implementation constraints.

        :return: The core element implementation constraints.
        """
        return self.impl_constraints

    def get_impl_type(self) -> typing.Optional[str]:
        """Get the core element implementation type.

        :return: The core element implementation type.
        """
        return self.impl_type

    def get_impl_local(self) -> bool:
        """Get the core element implementation local.

        :return: The core element implementation local.
        """
        return self.impl_local

    def get_impl_io(self) -> bool:
        """Get the core element implementation IO.

        :return: The core element implementation IO.
        """
        return self.impl_io

    def get_impl_type_args(self) -> typing.List[str]:
        """Get the core element implementation type arguments.

        :return: The core element implementation type arguments.
        """
        return self.impl_type_args

    def get_impl_prolog(self) -> typing.List[str]:
        """Get the core element implementation prolog.

        :return: The core element implementation prolog.
        """
        return self.impl_prolog

    def get_impl_epilog(self) -> typing.List[str]:
        """Get the core element implementation epilog.

        :return: The core element implementation epilog.
        """
        return self.impl_epilog

    def get_impl_container(self) -> typing.List[str]:
        """Get the core element implementation container.

        :return: The core element implementation container.
        """
        return self.impl_container

    ###########
    # SETTERS #
    ###########

    def set_ce_signature(self, ce_signature: str) -> None:
        """Set the core element signature.

        :param ce_signature: The core element signature.
        :returns: None.
        """
        self.ce_signature = ce_signature

    def set_impl_signature(self, impl_signature: str) -> None:
        """Set the core element implementation signature.

        :param impl_signature: The implementation signature.
        :return: None.
        """
        self.impl_signature = impl_signature

    def set_impl_constraints(
        self, impl_constraints: typing.Dict[str, typing.Union[str, int]]
    ) -> None:
        """Set the core element implementation constraints.

        :param impl_constraints: The implementation constraints.
        :return: None.
        """
        self.impl_constraints = impl_constraints

    def set_impl_type(self, impl_type: str) -> None:
        """Set the core element implementation type.

        :param impl_type: The implementation type.
        :return: None.
        """
        self.impl_type = impl_type

    def set_impl_local(self, impl_local: bool) -> None:
        """Set the core element implementation local.

        :param impl_local: The implementation local.
        :return: None.
        """
        self.impl_local = impl_local

    def set_impl_io(self, impl_io: bool) -> None:
        """Set the core element implementation IO.

        :param impl_io: The implementation IO.
        :return: None.
        """
        self.impl_io = impl_io

    def set_impl_type_args(self, impl_type_args: list) -> None:
        """Set the core element implementation type arguments.

        :param impl_type_args: The implementation type arguments.
        :return: None.
        """
        self.impl_type_args = impl_type_args

    def set_impl_prolog(self, impl_prolog: list) -> None:
        """Set the core element implementation prolog.

        :param impl_prolog: The implementation prolog.
        :return: None.
        """
        self.impl_prolog = impl_prolog

    def set_impl_epilog(self, impl_epilog: list) -> None:
        """Set the core element implementation epilog.

        :param impl_epilog: The implementation epilog.
        :return: None.
        """
        self.impl_epilog = impl_epilog

    def set_impl_container(self, impl_container: list) -> None:
        """Set the core element implementation container.

        :param impl_container: The implementation container.
        :return: None.
        """
        self.impl_container = impl_container

    ##################
    # REPRESENTATION #
    ##################

    def __repr__(self) -> str:
        """Build the element representation as string.

        :return: The core element representation.
        """
        _repr = "CORE ELEMENT: \n"
        _repr += f"\t - CE signature     : {str(self.ce_signature)}\n"
        _repr += f"\t - Impl. signature  : {str(self.impl_signature)}\n"
        if self.impl_constraints:
            impl_constraints = ""
            for key, value in self.impl_constraints.items():
                impl_constraints += f"{key}:{str(value)};"
        else:
            impl_constraints = str(self.impl_constraints)
        _repr += f"\t - Impl. constraints: {impl_constraints}\n"
        _repr += f"\t - Impl. type       : {str(self.impl_type)}\n"
        _repr += f"\t - Impl. local      : {str(self.impl_local)}\n"
        _repr += f"\t - Impl. io         : {str(self.impl_io)}\n"
        _repr += f"\t - Impl. prolog     : {str(self.impl_prolog)}\n"
        _repr += f"\t - Impl. epilog     : {str(self.impl_epilog)}\n"
        _repr += f"\t - Impl. container  : {str(self.impl_container)}\n"
        _repr += f"\t - Impl. type args  : {str(self.impl_type_args)}\n"
        return _repr
