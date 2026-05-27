/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
#ifndef PARAM_METADATA_H
#define PARAM_METADATA_H

// MATCHES JAVA COMPSsRuntime API Enum
// Adds runtime's internal data representations
#include "runtime_semantics.h"

// Converts directions to access mode representations
enum direction {
    in_dir = ACCESS_MODE_READ,
    out_dir = ACCESS_MODE_GENERATE,
    inout_dir = ACCESS_MODE_UPDATE,
    concurrent_dir = ACCESS_MODE_CONCURRENT_UPDATE,
    commutative_dir = ACCESS_MODE_COMMUTATIVE_UPDATE,
    in_delete_dir = ACCESS_MODE_READ_AND_DELETE,
    null_dir = -1
};

// Converts std IO streams annotations to runtime's internal representations
enum io_stream {
    STD_IN = STD_IO_STREAM_STDIN,
    STD_OUT = STD_IO_STREAM_STDOUT,
    STD_ERR = STD_IO_STREAM_STDERR,
    UNSPECIFIED = STD_IO_STREAM_UNSPECIFIED
};

// Converts onFailure to task failure policies respresentations
enum failure_policy {
    RETRY = FAILURE_POLICY_RETRY,
    IGNORE = FAILURE_POLICY_IGNORE,
    FAIL = FAILURE_POLICY_FAIL,
    CANCEL_SUCCESSORS = FAILURE_POLICY_CANCEL_SUCCESSORS
};

// Converts data types to runtime's internal representations
enum datatype {
    boolean_dt = DATA_TYPE_BOOLEAN_T,
    char_dt = DATA_TYPE_CHAR_T,
    byte_dt = DATA_TYPE_BYTE_T,
    short_dt = DATA_TYPE_SHORT_T,
    int_dt = DATA_TYPE_INT_T,
    long_dt = DATA_TYPE_LONG_T,
    float_dt = DATA_TYPE_FLOAT_T,
    double_dt = DATA_TYPE_DOUBLE_T,
    string_dt = DATA_TYPE_STRING_T,
    string_64_dt = DATA_TYPE_STRING_64_T,
    file_dt = DATA_TYPE_FILE_T,
    object_dt = DATA_TYPE_OBJECT_T,
    psco_dt = DATA_TYPE_PSCO_T,
    external_psco_dt = DATA_TYPE_EXTERNAL_PSCO_T,
    binding_object_dt = DATA_TYPE_BINDING_OBJECT_T,
    wchar_dt = DATA_TYPE_WCHAR_T,
    wstring_dt = DATA_TYPE_WSTRING_T,
    longlong_dt = DATA_TYPE_LONGLONG_T,
    void_dt = DATA_TYPE_VOID_T,
    any_dt = DATA_TYPE_ANY_T,
    array_char_dt = DATA_TYPE_ARRAY_CHAR_T,
    array_byte_dt = DATA_TYPE_ARRAY_BYTE_T,
    array_short_dt = DATA_TYPE_ARRAY_SHORT_T,
    array_int_dt = DATA_TYPE_ARRAY_INT_T,
    array_long_dt = DATA_TYPE_ARRAY_LONG_T,
    array_float_dt = DATA_TYPE_ARRAY_FLOAT_T,
    array_double_dt = DATA_TYPE_ARRAY_DOUBLE_T,
    collection_dt = DATA_TYPE_COLLECTION_T,
    dict_collection_dt = DATA_TYPE_DICT_COLLECTION_T,
    stream_dt = DATA_TYPE_STREAM_T,
    external_stream_dt = DATA_TYPE_EXTERNAL_STREAM_T,
    enum_dt = DATA_TYPE_ENUM_T,
    null_dt = DATA_TYPE_NULL_T,
    directory_dt = DATA_TYPE_DIRECTORY_T
};

#endif /* PARAM_METADATA_H */
