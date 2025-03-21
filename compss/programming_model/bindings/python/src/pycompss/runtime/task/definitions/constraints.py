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
PyCOMPSs runtime - Task - Definitions - Constraints.

This file contains the Constraints class, needed for the task registration.
"""

from pycompss.util.typing_helper import typing


class ConstraintDescription:
    """Constraint Description class."""

    __slots__ = [
        "param_name",
        "is_static",
    ]

    def __init__(
        self,
        param_name: typing.Union[str, int],
        is_static: bool = True,
    ) -> None:
        """Constraint Description constructor.

        :param param_name: Parameter name of the constraint.
        :param is_static: If the constraint is static.
        """
        self.param_name = param_name
        self.is_static = is_static

    ###########
    # METHODS #
    ###########

    def reset(self) -> None:
        """Reset the constraint description.

        :returns: None.
        """
        self.param_name = ""
        self.is_static = True

    ###########
    # GETTERS #
    ###########

    def get_param_name(self) -> typing.Union[str, int]:
        """Get the parameter name.

        :return: The parameter name.
        """
        return self.param_name

    def get_is_static(self) -> bool:
        """Get if the constraint is static.

        :return: The state of the constraint.
        """
        return self.is_static

    ###########
    # SETTERS #
    ###########

    def set_param_name(self, param_name: typing.Union[str, int]) -> None:
        """Set the param name.

        :param param_name: The name of the parameter.
        :returns: None.
        """
        self.param_name = param_name

    def set_is_static(self, is_static: bool) -> None:
        """Set if is static.

        :param is_static: The bool if its static.
        :return: None.
        """
        self.is_static = is_static

    ##################
    # REPRESENTATION #
    ##################

    def __repr__(self) -> str:
        """Build the element representation as string.

        :return: The constraint description representation.
        """
        _repr = "Constraint Description: \n"
        _repr += f"\t - Parameter name   : {str(self.param_name)}\n"
        _repr += f"\t - Static           : {str(self.is_static)}\n"

        return _repr
