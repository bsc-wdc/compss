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

#ifndef COMPSS_INTERFACE_H
#define COMPSS_INTERFACE_H

typedef struct CompssWorkflow CompssWorkflow;
typedef struct CompssInterface CompssInterface;

struct CompssWorkflow {
    long (*getId) (
        struct CompssWorkflow* self
    );

    void (*deregister)(
        struct CompssWorkflow* self
    );

    void (*openTaskGroup)(
        struct CompssWorkflow* self,
        const char* groupName,
        bool implicitBarrier
    );

    void (*closeTaskGroup)(
        struct CompssWorkflow* self,
        const char* groupName
    );

    void (*executeTask)(
        struct CompssWorkflow* self,
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

    void (*executeHttpTask)(
        struct CompssWorkflow* self,
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

    void (*cancelTaskGroup)(
        struct CompssWorkflow* self,
        const char* groupName,
        char** exceptionMessage
    );

    void (*cancelApplicationTasks)(
        struct CompssWorkflow* self
    );

    void (*noMoreTasks)(
        struct CompssWorkflow* self
    );

    void (*barrier)(
        struct CompssWorkflow* self
    );

    void (*barrierWithFlag)(
        struct CompssWorkflow* self,
        bool noMoreTasksFlag
    );

    void (*barrierGroup)(
        struct CompssWorkflow* self,
        const char* groupName,
        char** exceptionMessage
    );

    void (*snapshot)(
        struct CompssWorkflow* self
    );

    void (*get_object)(
        struct CompssWorkflow* self,
        char* objectId,
        char** buf
    );

    void (*delete_object)(
        struct CompssWorkflow* self,
        char* objectId,
        int** buf
    );
    
    int (*is_file_accessed)(
        struct CompssWorkflow* self,
        char* fileName
    );


    void (*open_file)(
        struct CompssWorkflow* self,
        char* fileName,
        int mode,
        char** buf
    );

    void (*get_file)(
        struct CompssWorkflow* self,
        char* fileName
    );

    void (*close_file)(
        struct CompssWorkflow* self,
        char* fileName,
        int mode
    );

    bool (*delete_file)(
        struct CompssWorkflow* self,
        char* fileName,
        int waitForData,
        int applicationDelete
    );

    void (*get_directory)(
        struct CompssWorkflow* self,
        char* dirName
    );

};

struct CompssInterface {

    // COMPSs Runtime state
    void (*On)(
    );
    
    void (*Off)(
        int code
    );

    void (*read_command)(char** command);

    CompssWorkflow* (*registerWorkflow)(
    );

    void (*RegisterCE)(
        char* ceSignature,
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

    void (*Get_AppDir)(
        char** buf
    );

    void (*Get_MasterWorkingDir)(
        char** buf
    );

    void (*EmitEvent)(
        int type,
        long id
    );

    void (*Set_wall_clock)(
        long appId,
        long wcl,
        int stopRT
    );

};

#endif // COMPSS_INTERFACE_H
