#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
DATATYPE_MAP = {
    # COMPSs types
    "BOOLEAN_T": "Boolean",
    "CHAR_T": "Text",
    "BYTE_T": "Integer",
    "SHORT_T": "Integer",
    "INT_T": "Integer",
    "LONG_T": "Integer",
    "FLOAT_T": "Float",
    "DOUBLE_T": "Float",
    "STRING_T": "Text",
    "STRING_64_T": "Text",
    "FILE_T": "File",
    "OBJECT_T": "DataType",
    "PSCO_T": "DataType",
    "EXTERNAL_PSCO_T": "DataType",
    "BINDING_OBJECT_T": "DataType",
    "WCHAR_T": "Text",
    "WSTRING_T": "Text",
    "LONGLONG_T": "Integer",
    "VOID_T": "Null",
    "ANY_T": "DataType",
    "ARRAY_CHAR_T": "Text",
    "ARRAY_BYTE_T": "Text",
    "ARRAY_SHORT_T": "Integer",
    "ARRAY_INT_T": "Integer",
    "ARRAY_LONG_T": "Integer",
    "ARRAY_FLOAT_T": "Float",
    "ARRAY_DOUBLE_T": "Float",
    "COLLECTION_T": "DataType",
    "DICT_COLLECTION_T": "DataType",
    "STREAM_T": "DataType",
    "EXTERNAL_STREAM_T": "DataType",
    "ENUM_T": "Text",
    "NULL_T": "Null",
    "DIRECTORY_T": "Dataset",
    "Future": "Future",

    # Python types
    "bool": "Boolean",
    "int": "Integer",
    "float": "Float",
    "complex": "Number",
    "str": "Text",
    "bytes": "Text",
    "bytearray": "Text",
    "NoneType": "Null",
    "file": "File",

    # Numpy types
    "int8": ["Integer", "numpy.int8"],
    "int16": ["Integer", "numpy.int16"],
    "int32": ["Integer", "numpy.int32"],
    "int64": ["Integer", "numpy.int64"],
    "uint8": ["Integer", "numpy.uint8"],
    "uint16": "Integer",
    "uint32": "Integer",
    "uint64": "Integer",
    "float16": "Float",
    "float32": "Float",
    "float64": "Float",
    "complex64": "Number",
    "complex128": "Number",
    "str_": "Text",
    "unicode_": "Text",
    "bytes_": "Text",
    "datetime64": "DateTime",
    "void": "Null",

    # Java primitive types
    "boolean": "Boolean",
    "byte": "Integer",
    "short": "Integer",
    "int": "Integer",
    "long": "Integer",
    "float": "Float",
    "double": "Float",
    "char": "Text",
    "void": "Null",

    # Java wrapper classes
    "java.lang.Boolean": "Boolean",
    "java.lang.Byte": "Integer",
    "java.lang.Short": "Integer",
    "java.lang.Integer": "Integer",
    "java.lang.Long": "Integer",
    "java.lang.Float": "Float",
    "java.lang.Double": "Float",
    "java.lang.Character": "Text",
    "java.lang.String": "Text",
    "java.lang.Number": "Number",
    "java.lang.Object": "DataType",
    "java.lang.Void": "Null",
}

DIRECTION_MAP = {
    "0": "IN",
    "1": "OUT",
    "2": "INOUT"
}


def map_datatype(custom_type: str) -> str:
    """ Maps a custom COMPSs or Python datatype to a Schema.org type. """
    custom_type = custom_type.replace("class", "").strip()
    return DATATYPE_MAP.get(custom_type, "DataType")    # Default fallback is the general 'DataType'


def map_direction(custom_type: str) -> str:
    """ Maps COMPSs direction codes to their human-readable form. """
    return DIRECTION_MAP.get(custom_type, custom_type)  # Default fallback is the original value


def allows_multiple_values(custom_type: str) -> bool:
    """ Checks if the given COMPSs type is a list or not """
    return custom_type.startswith("ARRAY") or "COLLECTION" in custom_type
