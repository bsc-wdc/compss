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
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>
#include "common.h"

using namespace std;

int DEBUG = 0;
int PERSISTENT = 0;

void init_env_vars() {
    char* dbg_str = getenv("COMPSS_BINDINGS_DEBUG");
    if (dbg_str != NULL && strcmp(dbg_str,"1")==0 ) {
        printf("Binding debug is activated\n");
        DEBUG = 1;
    }
    char* pers_str = getenv("COMPSS_PERSISTENT_BINDING");
    if (pers_str != NULL && strcmp(pers_str,"1")==0) {
        printf("Binding persistent is activated\n");
        PERSISTENT = 1;
    }
}

int is_debug() {
    return DEBUG;
}

int is_persistent() {
    return PERSISTENT;
}

char* concat(const char* s1, const char* s2) {
    const size_t len1 = strlen(s1);
    const size_t len2 = strlen(s2);
    char* result = (char*) malloc(len1 + len2 + 1); //+1 for the null-terminator
    // In real code you would check for errors in malloc here
    memcpy(result, s1, len1);
    memcpy(result + len1, s2, len2 + 1); //+1 to copy the null-terminator

    return result;
}
