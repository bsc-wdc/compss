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
#include <vector>
#include <sstream>
#include <fstream>

#include "GS_compss.h"
#include "compss_jni.h"
#include "compss_pipes.h"
#include "param_metadata.h"
#include "BindingDataManager.h"

using namespace std;

int PIPES=0;


// ******************************
// API functions
// ******************************
void GS_set_pipes(char* comPipe, char* resPipe){
	PIPES=1;
	PIPE_set_pipes(comPipe, resPipe);
}

void GS_read_pipes(char** command){
	if (PIPES){
		PIPE_read_command(command);
	}
}

void GS_On(AbstractCache* absCache) {
  init_data_manager(absCache);
  GS_On();
}


void GS_On() {
  if (PIPES){
  	PIPE_On();
  }else{
  	JNI_On();
  }
}


void GS_Off(int code) {
  if (PIPES) {
  	PIPE_Off(code);
  } else {
  	JNI_Off(code);
  }
}


void GS_Cancel_Application_Tasks(long appId) {
	if (PIPES) {
    PIPE_Cancel_Application_Tasks(appId);
	}else{
		JNI_Cancel_Application_Tasks(appId);
	}
}


void GS_Get_AppDir(char** buf) {
	if (PIPES) {
		PIPE_Get_AppDir(buf);
	} else {
		JNI_Get_AppDir(buf);
	}
}

void GS_Get_MasterWorkingDir(char** buf) {
	if (PIPES) {
		PIPE_Get_MasterWorkingDir(buf);
	} else {
		JNI_Get_MasterWorkingDir(buf);
	}
}


void GS_ExecuteTask(long appId, char* className, char* onFailure, int timeout, char* methodName, int priority, int numNodes, int reduce, int reduceChunkSize,
		int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	if (PIPES) {
		PIPE_ExecuteTask(appId, className, onFailure, timeout, methodName, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
	} else {
		JNI_ExecuteTask(appId, className, onFailure, timeout, methodName, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
	}
}


void GS_ExecuteTaskNew(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                       int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	if (PIPES) {
		PIPE_ExecuteTaskNew(appId, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
	} else {
		JNI_ExecuteTaskNew(appId, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
	}
}


void GS_ExecuteHttpTask(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                        int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    JNI_ExecuteHttpTask(appId, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}


void GS_RegisterCE(char* ceSignature, char* implSignature, char* implConstraints, char* implType, char* implLocal, char* implIO, char** prolog, char** epilog, char** container, int numArgs, char** implTypeArgs) {
	if (PIPES) {
	    // todo: is it necessary here too?
		PIPE_RegisterCE(ceSignature, implSignature, implConstraints, implType, implLocal, implIO, prolog, epilog, container, numArgs, implTypeArgs);
	} else {
		JNI_RegisterCE(ceSignature, implSignature, implConstraints, implType, implLocal, implIO, prolog, epilog, container, numArgs, implTypeArgs);
	}
}


int GS_Accessed_File(long appId, char* fileName){
	if (PIPES) {
		return PIPE_Accessed_File(appId, fileName);
	} else {
		return JNI_Accessed_File(appId, fileName);
	}
}


void GS_Open_File(long appId, char* fileName, int mode, char** buf) {
	if (PIPES) {
		PIPE_Open_File(appId, fileName, mode, buf);
	} else {
		JNI_Open_File(appId, fileName, mode, buf);
	}
}


void GS_Close_File(long appId, char* fileName, int mode) {
	if (PIPES) {
		PIPE_Close_File(appId, fileName, mode);
	} else {
		JNI_Close_File(appId, fileName, mode);
	}
}


void GS_Delete_File(long appId, char* fileName, int wait, int applicationDelete) {
	if (PIPES) {
		PIPE_Delete_File(appId, fileName, wait, applicationDelete);
	} else {
		JNI_Delete_File(appId, fileName, wait, applicationDelete);
	}
}


void GS_Get_File(long appId, char* fileName) {
	if (PIPES) {
		PIPE_Get_File(appId, fileName);
	} else {
		JNI_Get_File(appId, fileName);
	}
}


void GS_Get_Directory(long appId, char* dirName) {
	if (PIPES) {
		PIPE_Get_Directory(appId, dirName);
	} else {
		JNI_Get_Directory(appId, dirName);
	}
}


void GS_Get_Object(long appId, char* fileName, char** buf) {
	if (PIPES) {
		PIPE_Get_Object(appId, fileName, buf);
	} else {
		JNI_Get_Object(appId, fileName, buf);
	}
}


void GS_Delete_Object(long appId, char* fileName, int** buf) {
	if (PIPES) {
		PIPE_Delete_Object(appId, fileName, buf);
	} else {
		JNI_Delete_Object(appId, fileName, buf);
	}
}


void GS_Barrier(long appId) {
	if (PIPES) {
		PIPE_Barrier(appId);
	} else {
		JNI_Barrier(appId);
	}
}


void GS_BarrierNew(long appId, int noMoreTasks) {
	if (PIPES) {
		PIPE_BarrierNew(appId, noMoreTasks);
	} else {
		JNI_BarrierNew(appId, noMoreTasks);
	}
}


void GS_Snapshot(long appId) {
	if (PIPES) {
		PIPE_Snapshot(appId);
	} else {
		JNI_Snapshot(appId);
	}
}


void GS_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
	if (PIPES) {
		PIPE_BarrierGroup(appId, groupName, exceptionMessage);
	} else {
		JNI_BarrierGroup(appId, groupName, exceptionMessage);
	}
}


void GS_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
	if (PIPES) {
		PIPE_OpenTaskGroup(groupName, implicitBarrier, appId);
	} else {
		JNI_OpenTaskGroup(groupName, implicitBarrier, appId);
	}
}


void GS_CloseTaskGroup(char* groupName, long appId){
	if (PIPES) {
		PIPE_CloseTaskGroup(groupName, appId);
	} else {
		JNI_CloseTaskGroup(groupName, appId);
	}
}

void GS_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
	if (PIPES) {
		PIPE_CancelTaskGroup(groupName, appId, exceptionMessage);
	} else {
		JNI_CancelTaskGroup(groupName, appId, exceptionMessage);
	}
}


void GS_EmitEvent(int type, long id) {
	if (PIPES) {
		PIPE_EmitEvent(type, id);
	} else {
		JNI_EmitEvent(type, id);
	}
}


int GS_GetNumberOfResources(long appId) {
	if (PIPES) {
		return PIPE_GetNumberOfResources(appId);
	} else {
		return JNI_GetNumberOfResources(appId);
	}
}


void GS_RequestResources(long appId, int numResources, char* groupName) {
	if (PIPES) {
		PIPE_RequestResources(appId, numResources, groupName);
	} else {
		JNI_RequestResources(appId, numResources, groupName);
	}
}


void GS_FreeResources(long appId, int numResources, char* groupName) {
	if (PIPES) {
		PIPE_FreeResources(appId, numResources, groupName);
	} else {
		JNI_FreeResources(appId, numResources, groupName);
	}
}

void GS_Set_wall_clock(long appId, long wcl, int stopRT){
	if (PIPES) {
		PIPE_set_wall_clock(appId, wcl, stopRT);
	} else {
		JNI_set_wall_clock(appId, wcl, stopRT);
	}
}
