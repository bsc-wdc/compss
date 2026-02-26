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

#include <fstream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <pthread.h>

#include "BindingDataManager.h"
#include "GS_compss.h"
#include "compss_interface.h"
#include "compss_jni.h"
#include "compss_pipes.h"
#include "compss_sockets.h"
#include "param_metadata.h"
#include "compss_interface.h"
#include "BindingDataManager.h"

using namespace std;

CompssInterface runtime;
pthread_mutex_t workflow_mutex = PTHREAD_MUTEX_INITIALIZER;
CompssWorkflow* workflow = NULL;
long wf_appId = -1;

void registerWorkflow() {
	pthread_mutex_lock(&workflow_mutex);
	// double-check to ensure that other threads hadn't registered the workflow while waiting
	if (workflow == NULL) {  
		workflow = runtime.registerWorkflow();
		wf_appId = workflow->getId(workflow);
	}
	pthread_mutex_unlock(&workflow_mutex);
}

// ******************************
// API functions
// ******************************
void GS_set_pipes(char* comPipe, char* resPipe){
	runtime = setup_PIPE_runtime(comPipe, resPipe);
}

void GS_set_socket_endpoint(char* endpoint) {
    runtime = setup_SOCKET_runtime(endpoint);
}

void GS_set_JNI_runtime(void) {
    runtime = setup_JNI_runtime();
} 

void GS_read_command(char **command) {
	runtime.read_command(command);
}

void GS_On(AbstractCache* absCache) {
    init_data_manager(absCache);
    GS_On();
}

void GS_On() {
	runtime.On();
}

void GS_Off(int code) {
	runtime.Off(code);
}

void GS_Get_AppDir(char** buf) {
	runtime.Get_AppDir(buf);
}

void GS_Get_MasterWorkingDir(char** buf) {
	runtime.Get_MasterWorkingDir(buf);
}

void GS_RegisterCE(char* ceSignature, char* implSignature, char* implConstraints, char* implType, char* implLocal, char* implIO, char** prolog, char** epilog, char** container, int numArgs, char** implTypeArgs) {
	runtime.RegisterCE(ceSignature, implSignature, implConstraints, implType, implLocal, implIO, prolog, epilog, container, numArgs, implTypeArgs);
}

void GS_EmitEvent(int type, long id) {
	runtime.EmitEvent(type, id);
}

void GS_Set_wall_clock(long appId, long wcl, int stopRT){
	if (workflow == NULL) {
		registerWorkflow();
    }
	runtime.Set_wall_clock(wf_appId, wcl, stopRT);
}

void GS_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
	if (workflow == NULL) {
		registerWorkflow();
    }
	workflow->openTaskGroup(workflow, groupName, implicitBarrier);
}

void GS_CloseTaskGroup(char* groupName, long appId){
	if (workflow != NULL){
		workflow->closeTaskGroup(workflow, groupName);
	}
}

void GS_ExecuteTask(long appId, char* className, char* onFailure, int timeout, char* methodName, int priority, int numNodes, int reduce, int reduceChunkSize,
		int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	if (workflow == NULL) {
		registerWorkflow();
    }
	workflow->executeTask(workflow, className, onFailure, timeout, methodName, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}

void GS_ExecuteTaskNew(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                       int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	if (workflow == NULL) {
		registerWorkflow();
    }
	workflow->executeTaskNew(workflow, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}

void GS_ExecuteHttpTask(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                        int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	if (workflow == NULL) {
		registerWorkflow();
    }
    workflow->executeHttpTask(workflow, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}

void GS_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
	if (workflow != NULL){
		workflow->cancelTaskGroup(workflow, groupName, exceptionMessage);
	}
}

void GS_Cancel_Application_Tasks(long appId) {
	if (workflow != NULL){
		workflow->cancelApplicationTasks(workflow);
	}
}

void GS_Barrier(long appId) {
	if (workflow != NULL){
		workflow->barrier(workflow);
	}
}

void GS_BarrierNew(long appId, int noMoreTasks) {
	if (workflow != NULL){
		workflow->barrierWithFlag(workflow, noMoreTasks);
	}
}

void GS_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
	if (workflow != NULL){
		workflow->barrierGroup(workflow, groupName, exceptionMessage);
	}
}

void GS_Snapshot(long appId) {
	if (workflow != NULL){
		workflow->snapshot(workflow);
	}
}

void GS_Get_Object(long appId, char* fileName, char** buf) {
	if (workflow != NULL){
		workflow->get_object(workflow, fileName, buf);
	} else {
		*buf = fileName;
	}
}

void GS_Delete_Object(long appId, char* fileName, int** buf) {
	if (workflow != NULL){
		workflow->delete_object(workflow, fileName, buf);
	} else {
		*buf = NULL;
	}
}

int GS_Accessed_File(long appId, char* fileName){
	if (workflow != NULL){
		return workflow->is_file_accessed(workflow, fileName);
	}
	return 0;
}

void GS_Open_File(long appId, char* fileName, int mode, char** buf) {
	if (workflow != NULL){
		workflow->open_file(workflow, fileName, mode, buf);
	} else {
		*buf = fileName;
	}
}

void GS_Get_File(long appId, char* fileName) {
	if (workflow != NULL){
		workflow->get_file(workflow, fileName);
	}
}

void GS_Close_File(long appId, char* fileName, int mode) {
	if (workflow != NULL){
		workflow->close_file(workflow, fileName, mode);
	}
}

void GS_Delete_File(long appId, char* fileName, int wait, int applicationDelete) {
	if (workflow != NULL){
		workflow->delete_file(workflow, fileName, wait, applicationDelete);
	}
}

void GS_Get_Directory(long appId, char* dirName) {
	if (workflow != NULL){
		workflow->get_directory(workflow, dirName);
	}
}
