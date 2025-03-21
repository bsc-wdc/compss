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
#ifndef PIPE_COMPSS_H
#define PIPE_COMPSS_H


#include "common.h"


/*** ==============> API FUNCTIONS <================= ***/

// COMPSs Runtime state
void PIPE_On(void);
void PIPE_set_pipes(char* inPipe, char* outPipe);
void PIPE_read_command(char** command);
void PIPE_Off(int code);
void PIPE_Cancel_Application_Tasks(long appId);

// Task methods
void PIPE_RegisterCE(char* ceSignature,
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
void PIPE_ExecuteTask(long appId,
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
void PIPE_ExecuteTaskNew(long appId,
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
int PIPE_Accessed_File(long appId, char* fileName);
void PIPE_Open_File(long appId, char* fileName, int mode, char** buf);
void PIPE_Close_File(long appId, char* fileName, int mode);
void PIPE_Delete_File(long appId, char* fileName, int waitForData, int applicationDelete);
void PIPE_Get_File(long appId, char* fileName);

void PIPE_Get_Directory(long appId, char* dirName);

// COMPSs API Calls
void PIPE_Barrier(long appId);
void PIPE_BarrierNew(long appId, int noMoreTasks);
void PIPE_BarrierGroup(long appId, char* groupName, char** exceptionMessage);
void PIPE_OpenTaskGroup(char* groupName, int implicitBarrier, long appId);
void PIPE_CloseTaskGroup(char* groupName, long appId);
void PIPE_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage);
void PIPE_Snapshot(long appId);
int PIPE_GetNumberOfResources(long appId);
void PIPE_RequestResources(long appId, int numResources, char* groupName);
void PIPE_FreeResources(long appId, int numResources, char* groupName);

// Misc functions
void PIPE_Get_AppDir(char** buf);
void PIPE_Get_MasterWorkingDir(char** buf);
void PIPE_EmitEvent(int type, long id);
void PIPE_Get_Object(long appId, char* objectId, char** buf);
void PIPE_Delete_Object(long appId, char* objectId, int** buf);
void PIPE_set_wall_clock(long appId, long wcl, int stopRT);

#endif /* PIPE_COMPSS_H */
