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
#ifndef JNI_COMPSS_H
#define JNI_COMPSS_H


#include "common.h"
#include "common_jni.h"

/*** ==============> API FUNCTIONS <================= ***/

// COMPSs Runtime state
void JNI_On(void);
void JNI_Off(int code);
void JNI_Cancel_Application_Tasks(long appId);

// Task methods
void JNI_RegisterCE(char* ceSignature,
                              char* implSignature,
                              char* implConstraints,
                              char* implType,
                              char* implLocal,
                              char* implIO,
                              char** prolog,
                              char** epilog,
                              char** container,
                              int numParams,
                              char** implTypeArgs
                             );
void JNI_ExecuteTask(long appId,
                               char* className,
                               char* onFailure,
                               int timeout,
                               char* methodName,
                               int priority,
							   int numNodes,
							   int reduce, int reduceChunkSize,
							   int replicated, int distributed,
                               int hasTarget,
                               int numReturns,
			                   int numParams,
                               void** params
                              );
void JNI_ExecuteTaskNew(long appId,
                                  char* signature,
                                  char* onFailure,
                                  int timeout,
                                  int priority,
                                  int numNodes,
                                  int reduce,
                                  int reduceChunkSize,
                                  int replicated,
                                  int distributed,
                                  int hasTarget,
                                  int numReturns,
                                  int numParams,
                                  void** params
                                 );

void JNI_ExecuteHttpTask(long appId,
                         char* signature,
                         char* onFailure,
                         int timeout,
                         int priority,
                         int numNodes,
                         int reduce,
                         int reduceChunkSize,
                         int replicated,
                         int distributed,
                         int hasTarget,
                         int numReturns,
                         int numParams,
                         void** params
                         );

// File methods
int JNI_Accessed_File(long appId, char* fileName);
void JNI_Open_File(long appId, char* fileName, int mode, char** buf);
void JNI_Close_File(long appId, char* fileName, int mode);
void JNI_Delete_File(long appId, char* fileName, int waitForData, int applicationDelete);
void JNI_Get_File(long appId, char* fileName);

void JNI_Get_Directory(long appId, char* dirName);

// COMPSs API Calls
void JNI_Barrier(long appId);
void JNI_BarrierNew(long appId, int noMoreTasks);
void JNI_BarrierGroup(long appId, char* groupName, char** exceptionMessage);
void JNI_OpenTaskGroup(char* groupName, int implicitBarrier, long appId);
void JNI_CloseTaskGroup(char* groupName, long appId);
void JNI_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage);
void JNI_Snapshot(long appId);
int JNI_GetNumberOfResources(long appId);
void JNI_RequestResources(long appId, int numResources, char* groupName);
void JNI_FreeResources(long appId, int numResources, char* groupName);

// Misc functions
void JNI_Get_AppDir(char** buf);
void JNI_Get_MasterWorkingDir(char** buf);
void JNI_EmitEvent(int type, long id);
void JNI_Get_Object(long appId, char* objectId, char** buf);
void JNI_Delete_Object(long appId, char* objectId, int** buf);
void JNI_set_wall_clock(long appId, long wcl, int stopRT);

#endif /* JNI_COMPSS_H */
