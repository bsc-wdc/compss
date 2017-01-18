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
@author: etejedor
@author: fconejer

PyCOMPSs API - Parameter
========================
    This file contains the clases needed for the parameter definition.
    1. Direction.
        - IN
        - OUT
        - INOUT
    2. Type.
        - FILE
        - BOOLEAN
        - STRING
        - INT
        - LONG
        - FLOAT
        - OBJECT
    3. Parameter.
"""


# Numbers match both C and Java enums
class Direction:
    IN = 0
    OUT = 1
    INOUT = 2


# Numbers match both C and Java enums
class Type:
    FILE = 0
    BOOLEAN = 1
    STRING = 3
    INT = 6
    LONG = 7
    FLOAT = 9    # C double
    OBJECT = 10
    # COMPLEX = 8


class Parameter:
    """
    Parameter class
    Used to group the type, direction and value of a parameter
    """
    def __init__(self, p_type=None, p_direction=Direction.IN):
        self.type = p_type
        self.direction = p_direction
        self.value = None    # placeholder for parameter value


# Aliases for parameters
IN = Parameter()
OUT = Parameter(p_direction=Direction.OUT)
INOUT = Parameter(p_direction=Direction.INOUT)

FILE = Parameter(p_type=Type.FILE)
FILE_IN = Parameter(p_type=Type.FILE)
FILE_OUT = Parameter(p_type=Type.FILE, p_direction=Direction.OUT)
FILE_INOUT = Parameter(p_type=Type.FILE, p_direction=Direction.INOUT)

# Java max and min integer and long values
JAVA_MAX_INT = 2147483647
JAVA_MIN_INT = -2147483648
JAVA_MAX_LONG = PYTHON_MAX_INT = 9223372036854775807
JAVA_MIN_LONG = PYTHON_MIN_INT = -9223372036854775808
