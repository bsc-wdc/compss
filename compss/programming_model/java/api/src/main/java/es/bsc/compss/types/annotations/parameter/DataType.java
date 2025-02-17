/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.annotations.parameter;

/**
 * Parameter types (for bindings and internal java).
 */
public enum DataType {
    BOOLEAN_T, // Boolean
    CHAR_T, // Char
    BYTE_T, // Byte
    SHORT_T, // Short
    INT_T, // Int
    LONG_T, // Long
    FLOAT_T, // Float
    DOUBLE_T, // Double
    STRING_T, // String
    STRING_64_T, // String
    FILE_T, // File
    OBJECT_T, // Java Object
    PSCO_T, // Java PSCO
    EXTERNAL_PSCO_T, // Bindings PSCO
    BINDING_OBJECT_T, // Binding Object
    WCHAR_T, // Wchar
    WSTRING_T, // Wstring
    LONGLONG_T, // Long long
    VOID_T, // Void
    ANY_T, // Any
    ARRAY_CHAR_T, // Array Char
    ARRAY_BYTE_T, // Array byte
    ARRAY_SHORT_T, // Array short
    ARRAY_INT_T, // Array int
    ARRAY_LONG_T, // Array long
    ARRAY_FLOAT_T, // Array float
    ARRAY_DOUBLE_T, // Array double
    COLLECTION_T, // Collection
    DICT_COLLECTION_T, // Dictionary Collection
    STREAM_T, // Streams
    EXTERNAL_STREAM_T, // Binding Streams
    ENUM_T, // Enum
    NULL_T, // Null
    DIRECTORY_T; // Directory
}
