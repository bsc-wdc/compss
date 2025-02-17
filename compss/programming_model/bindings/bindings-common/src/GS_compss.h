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
#ifndef GS_COMPSS_H
#define GS_COMPSS_H


#include "AbstractCache.h"
#include "common.h"

void GS_On(AbstractCache* absCache);

/*** ==============> API FUNCTIONS <================= ***/

// COMPSs Runtime state
extern "C" void GS_On(void);
extern "C" void GS_set_pipes(char* comPipe, char* resPipe);
extern "C" void GS_read_pipes(char** command);
extern "C" void GS_Off(int code);
extern "C" void GS_Cancel_Application_Tasks(long appId);

// Task methods
extern "C" void GS_RegisterCE(char* ceSignature,
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
extern "C" void GS_ExecuteTask(long appId,
                               char* className,
                               char* onFailure,
                               int timeout,
                               char* methodName,
                               int priority,
                               int hasTarget,
							   int numNodes,
							   int reduce, int reduceChunkSize,
							   int replicated, int distributed,
                               int numReturns,
			                   int numParams,
                               void** params
                              );
extern "C" void GS_ExecuteTaskNew(long appId,
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
extern "C" void GS_ExecuteHttpTask(long appId,
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
extern "C" int GS_Accessed_File(long appId, char* fileName);
extern "C" void GS_Open_File(long appId, char* fileName, int mode, char** buf);
extern "C" void GS_Close_File(long appId, char* fileName, int mode);
extern "C" void GS_Delete_File(long appId, char* fileName, int waitForData, int applicationDelete);
extern "C" void GS_Get_File(long appId, char* fileName);

extern "C" void GS_Get_Directory(long appId, char* dirName);

// COMPSs API Calls
extern "C" void GS_Barrier(long appId);
extern "C" void GS_BarrierNew(long appId, int noMoreTasks);
extern "C" void GS_BarrierGroup(long appId, char* groupName, char** exceptionMessage);
extern "C" void GS_OpenTaskGroup(char* groupName, int implicitBarrier, long appId);
extern "C" void GS_CloseTaskGroup(char* groupName, long appId);
extern "C" void GS_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage);
extern "C" void GS_Snapshot(long appId);
extern "C" int GS_GetNumberOfResources(long appId);
extern "C" void GS_RequestResources(long appId, int numResources, char* groupName);
extern "C" void GS_FreeResources(long appId, int numResources, char* groupName);

// Misc functions
extern "C" void GS_Get_AppDir(char** buf);
extern "C" void GS_Get_MasterWorkingDir(char** buf);
extern "C" void GS_EmitEvent(int type, long id);
extern "C" void GS_Get_Object(long appId, char* objectId, char** buf);
extern "C" void GS_Delete_Object(long appId, char* objectId, int** buf);
extern "C" void GS_Set_wall_clock(long appId, long wcl, int stopRT);

#endif /* GS_COMPSS_H */
