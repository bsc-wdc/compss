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
#ifndef PARAM_METADATA_H
#define PARAM_METADATA_H

// MATCHES JAVA COMPSsRuntime API Enum
// Adds internal data representations
#include "data_type.h"

// MATCHES JAVA COMPSsRuntime PARAMETER API Enum
// Adds internal data representations
enum direction {
    in_dir = 0,
    out_dir,
    inout_dir,
    concurrent_dir,
    commutative_dir,
	in_delete_dir,
    null_dir
};

// MATCHED JAVA COMPSsRuntime PARAMETER API Enum
// Adds stream representations
enum io_stream {
    STD_IN = 0,
    STD_OUT,
    STD_ERR,
    UNSPECIFIED
};

#endif /* PARAM_METADATA_H */
