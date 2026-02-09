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


void GS_read_pipes(char** command){
	GS_read_command(command);
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


void GS_Cancel_Application_Tasks(long appId) {
	runtime.Cancel_Application_Tasks(appId);
}


void GS_Get_AppDir(char** buf) {
	runtime.Get_AppDir(buf);
}

void GS_Get_MasterWorkingDir(char** buf) {
	runtime.Get_MasterWorkingDir(buf);
}


void GS_ExecuteTask(long appId, char* className, char* onFailure, int timeout, char* methodName, int priority, int numNodes, int reduce, int reduceChunkSize,
		int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	runtime.ExecuteTask(appId, className, onFailure, timeout, methodName, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}


void GS_ExecuteTaskNew(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                       int distributed, int hasTarget, int numReturns, int numParams, void** params) {
	runtime.ExecuteTaskNew(appId, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}


void GS_ExecuteHttpTask(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize, int replicated,
                        int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    runtime.ExecuteHttpTask(appId, signature, onFailure, timeout, priority, numNodes, reduce, reduceChunkSize, replicated, distributed, hasTarget, numReturns, numParams, params);
}


void GS_RegisterCE(char* ceSignature, char* implSignature, char* implConstraints, char* implType, char* implLocal, char* implIO, char** prolog, char** epilog, char** container, int numArgs, char** implTypeArgs) {
	runtime.RegisterCE(ceSignature, implSignature, implConstraints, implType, implLocal, implIO, prolog, epilog, container, numArgs, implTypeArgs);
}


int GS_Accessed_File(long appId, char* fileName){
	return runtime.Accessed_File(appId, fileName);
}


void GS_Open_File(long appId, char* fileName, int mode, char** buf) {
	runtime.Open_File(appId, fileName, mode, buf);
}


void GS_Close_File(long appId, char* fileName, int mode) {
	runtime.Close_File(appId, fileName, mode);
}


void GS_Delete_File(long appId, char* fileName, int wait, int applicationDelete) {
	runtime.Delete_File(appId, fileName, wait, applicationDelete);
}


void GS_Get_File(long appId, char* fileName) {
	runtime.Get_File(appId, fileName);
}


void GS_Get_Directory(long appId, char* dirName) {
	runtime.Get_Directory(appId, dirName);
}


void GS_Get_Object(long appId, char* fileName, char** buf) {
	runtime.Get_Object(appId, fileName, buf);
}


void GS_Delete_Object(long appId, char* fileName, int** buf) {
	runtime.Delete_Object(appId, fileName, buf);
}


void GS_Barrier(long appId) {
	runtime.Barrier(appId);
}


void GS_BarrierNew(long appId, int noMoreTasks) {
	runtime.BarrierNew(appId, noMoreTasks);
}


void GS_Snapshot(long appId) {
	runtime.Snapshot(appId);
}


void GS_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
	runtime.BarrierGroup(appId, groupName, exceptionMessage);
}


void GS_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
	runtime.OpenTaskGroup(groupName, implicitBarrier, appId);
}


void GS_CloseTaskGroup(char* groupName, long appId){
	runtime.CloseTaskGroup(groupName, appId);
}

void GS_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
	runtime.CancelTaskGroup(groupName, appId, exceptionMessage);
}


void GS_EmitEvent(int type, long id) {
	runtime.EmitEvent(type, id);
}


int GS_GetNumberOfResources(long appId) {
	return runtime.GetNumberOfResources(appId);
}


void GS_RequestResources(long appId, int numResources, char* groupName) {
	runtime.RequestResources(appId, numResources, groupName);
}


void GS_FreeResources(long appId, int numResources, char* groupName) {
	runtime.FreeResources(appId, numResources, groupName);
}

void GS_Set_wall_clock(long appId, long wcl, int stopRT){
	runtime.Set_wall_clock(appId, wcl, stopRT);
}
