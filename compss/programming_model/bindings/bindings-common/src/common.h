/*
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
#ifndef COMMONS_H
#define COMMONS_H

// Uncomment the following define to get debug information.
//#define DEBUG_BINDING

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args); fflush(stdout);
#else
#define debug_printf(args...) {}
#endif

char* concat(const char*, const char*);

#endif // COMMONS_H
