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
#ifndef TESTS_INTERNAL_COMPSS_SOCKETS_TEST_API_H
#define TESTS_INTERNAL_COMPSS_SOCKETS_TEST_API_H

// Internal entry points of compss_sockets.cc, exposed for white-box tests
// only. These are NOT part of the public bindings-common API and must not be
// included from production code.

struct CompssWorkflow;

typedef void (*SocketRetrySleepHook)(long seconds);

void SOCKET_set_endpoint(char* endpoint);
void SOCKET_set_retry_sleep_hook(SocketRetrySleepHook hook);
void SOCKET_clear_retry_sleep_hook(void);
void SOCKET_WF_cancelApplicationTasks(struct CompssWorkflow* self);

#endif // TESTS_INTERNAL_COMPSS_SOCKETS_TEST_API_H
