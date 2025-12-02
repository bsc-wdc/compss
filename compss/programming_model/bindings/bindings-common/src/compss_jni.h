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
#ifndef JNI_COMPSS_H
#define JNI_COMPSS_H


#include "common.h"
#include "common_jni.h"
#include "compss_interface.h"

CompssWorkflow* JNI_RegisterWorkflow();

long JNI_WF_getId(CompssWorkflow* wf);

void JNI_WF_deregister(CompssWorkflow* wf) ;

void JNI_WF_openTaskGroup(CompssWorkflow* wf, const char* groupName, bool implicitBarrier);

void JNI_WF_closeTaskGroup(CompssWorkflow* wf, const char* groupName);

void JNI_WF_cancelTaskGroup(CompssWorkflow* wf, const char* groupName, char** exceptionMessage);

void JNI_WF_cancelApplicationTasks(CompssWorkflow* wf);

void JNI_WF_noMoreTasks(CompssWorkflow* wf);

void JNI_WF_barrier(CompssWorkflow* wf);

void JNI_WF_barrierWithFlag(CompssWorkflow* wf, bool noMoreTasksFlag);

void JNI_WF_barrierGroup(CompssWorkflow* wf, const char* groupName, char** exceptionMessage);

void JNI_WF_snapshot(CompssWorkflow* wf);


CompssInterface setup_JNI_runtime(void);

#endif /* JNI_COMPSS_H */
