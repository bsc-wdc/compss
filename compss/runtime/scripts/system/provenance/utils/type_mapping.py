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
    "list": "DataType",
    "tuple": "DataType",
    "set": "DataType",
    "frozenset": "DataType",
    "dict": "DataType",
    "NoneType": "Null",
    "object": "Thing",
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
    "bool": "Boolean",
    "str_": "Text",
    "unicode_": "Text",
    "object": "Object",
    "bytes_": "Text",
    "datetime64": "DateTime",
    "void": "Null",
}

DIRECTION_MAP = {
    "0": "IN",
    "1": "OUT",
    "2": "INOUT"
}

def map_datatype(custom_type: str) -> str:
    """ Maps a custom COMPSs or Python datatype to a Schema.org type. """
    return DATATYPE_MAP.get(custom_type, custom_type)  # Default fallback is the original value


def map_direction(custom_type: str) -> str:
    """ Maps COMPSs direction codes to their human-readable form. """
    return DIRECTION_MAP.get(custom_type, custom_type)  # Default fallback is the original value


def allows_multiple_values(custom_type: str) -> bool:
    """ Checks if the given COMPSs type is a list or not """
    return custom_type.startswith("ARRAY") or "COLLECTION" in custom_type
