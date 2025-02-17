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
PyCOMPSs API - commons - implementation types.

This file contains the implementation types definitions.
"""


class _ImplementationTypes:  # pylint: disable=R0903,R0902
    # disable=too-few-public-methods, too-many-instance-attributes
    """Supported implementation types."""

    __slots__ = [
        "binary",
        "cet_binary",
        "compss",
        "container",
        "decaf",
        "julia",
        "method",
        "mpi",
        "multi_node",
        "ompss",
        "opencl",
        "python_mpi",
        "http",
    ]

    def __init__(self) -> None:
        self.binary = "BINARY"
        self.cet_binary = "CET_BINARY"
        self.compss = "COMPSs"
        self.container = "CONTAINER"
        self.decaf = "DECAF"
        self.julia = "JULIA"
        self.method = "METHOD"
        self.mpi = "MPI"
        self.multi_node = "MULTI_NODE"
        self.ompss = "OMPSS"
        self.opencl = "OPENCL"
        self.python_mpi = "PYTHON_MPI"
        self.http = "HTTP"


IMPLEMENTATION_TYPES = _ImplementationTypes()
