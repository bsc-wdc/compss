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

// Test-only translation unit. Provides a hook the transport harnesses use to drop
// the cached workflow between parameterized test cases so that switching
// transports does not leave behind function pointers tied to a previous
// transport's file descriptors.
//
// This file deliberately lives under tests/internal so it stays out of the
// shipped library. It pokes at GS_compss.cc's globals via extern declarations
// rather than adding a hook to production source.

#include <pthread.h>

#include "compss_interface.h"

// Globals defined in src/GS_compss.cc with external linkage.
extern CompssWorkflow* workflow;
extern long wf_appId;
extern pthread_mutex_t workflow_mutex;

// Free function defined in src/GS_compss.cc.
void registerWorkflow();

extern "C" void GS_test_reset_workflow(void) {
    pthread_mutex_lock(&workflow_mutex);
    workflow = NULL;
    wf_appId = -1;
    pthread_mutex_unlock(&workflow_mutex);
    registerWorkflow();
}
