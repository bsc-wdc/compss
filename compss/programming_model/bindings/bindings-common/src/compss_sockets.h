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
#ifndef SOCKETS_COMPSS_H
#define SOCKETS_COMPSS_H

#include "common.h"
#include "compss_interface.h"

CompssInterface setup_SOCKET_runtime(char* endpoint);

// Runtime control
void SOCKET_On(void);
void SOCKET_set_endpoint(char* endpoint);
void SOCKET_read_command(char** command);
void SOCKET_Off(int code);
void SOCKET_Cancel_Application_Tasks(long appId);

// Task registration / execution
void SOCKET_RegisterCE(char* ceSignature,
                       char* implSignature,
                       char* implConstraints,
                       char* implType,
                       char* implLocal,
                       char* implIO,
                       char** prolog,
                       char** epilog,
                       char** container,
                       int numParams,
                       char** implTypeArgs);

void SOCKET_ExecuteTask(long appId,
                        char* className,
                        char* onFailure,
                        int timeout,
                        char* methodName,
                        int priority,
                        int numNodes,
                        int reduce,
                        int reduceChunkSize,
                        int replicated,
                        int distributed,
                        int hasTarget,
                        int numReturns,
                        int numParams,
                        void** params);

void SOCKET_ExecuteTaskNew(long appId,
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
                           void** params);

// File operations
int SOCKET_Accessed_File(long appId, char* fileName);
void SOCKET_Open_File(long appId, char* fileName, int mode, char** buf);
void SOCKET_Close_File(long appId, char* fileName, int mode);
void SOCKET_Delete_File(long appId, char* fileName, int waitForData, int applicationDelete);
void SOCKET_Get_File(long appId, char* fileName);
void SOCKET_Get_Directory(long appId, char* dirName);

// Synchronisation / task groups
void SOCKET_Barrier(long appId);
void SOCKET_BarrierNew(long appId, int noMoreTasks);
void SOCKET_BarrierGroup(long appId, char* groupName, char** exceptionMessage);
void SOCKET_OpenTaskGroup(char* groupName, int implicitBarrier, long appId);
void SOCKET_CloseTaskGroup(char* groupName, long appId);
void SOCKET_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage);
void SOCKET_Snapshot(long appId);
int SOCKET_GetNumberOfResources(long appId);
void SOCKET_RequestResources(long appId, int numResources, char* groupName);
void SOCKET_FreeResources(long appId, int numResources, char* groupName);

// Miscellaneous
void SOCKET_Get_AppDir(char** buf);
void SOCKET_Get_MasterWorkingDir(char** buf);
void SOCKET_EmitEvent(int type, long id);
void SOCKET_Get_Object(long appId, char* objectId, char** buf);
void SOCKET_Delete_Object(long appId, char* objectId, int** buf);
void SOCKET_set_wall_clock(long appId, long wcl, int stopRT);

typedef void (*SocketRetrySleepHook)(long seconds);
void SOCKET_set_retry_sleep_hook(SocketRetrySleepHook hook);
void SOCKET_clear_retry_sleep_hook(void);

#endif /* SOCKETS_COMPSS_H */
