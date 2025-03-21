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
PyCOMPSs runtime - Task - Wrappers - psco_stream.

This file contains the PSCO Stream wrapper class.
"""

# ###########################################################################
# ### THIS IS TEMPORAL UNTIL THE EXTERNAL PSCO STREAM TYPE IS IMPLEMENTED ###
# ###########################################################################

from pycompss.util.typing_helper import typing


class PscoStreamWrapper:
    """PSCO Stream wrapper definition.

    This class represents a PSCO Stream object.
    """

    __slots__ = [
        "psco_id",
    ]

    def __init__(self, psco_id: typing.Union[str, None]) -> None:
        """Set the persistent object identifier in the placeholder."""
        self.psco_id = psco_id

    def get_psco_id(self) -> typing.Union[str, None]:
        """Retrieve the persistent object identifier."""
        return self.psco_id
