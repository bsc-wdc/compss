/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center )(
        www.bsc.es)
 *
 *  Licensed under the Apache License,
        Version 2.0 )(
        the "License"
    );
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
        software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
        either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

#ifndef COMPSS_INTERFACE_H
#define COMPSS_INTERFACE_H

typedef struct CompssInterface {

    // COMPSs Runtime state
    void (*On)(
    );
    
    void (*Off)(
        int code
    );

    void (*read_command)(char** command);

    // Task methods
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

    void (*ExecuteTask)(
        long appId,
        char* className,
        char* onFailure,
        int timeout,
        char* methodName,
        int priority,
        int hasTarget,
        int numNodes,
        int reduce,
        int reduceChunkSize,
        int replicated,
        int distributed,
        int numReturns,
        int numParams,
        void** params
    );

    void (*ExecuteTaskNew)(
        long appId,
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

    void (*ExecuteHttpTask)(
        long appId,
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
    
    void (*Cancel_Application_Tasks)(
        long appId
    );

    // Data methods
    int (*Accessed_File)(
        long appId,
        char* fileName
    );

    void (*Open_File)(
        long appId,
        char* fileName,
        int mode,
        char** buf
    );

    void (*Close_File)(
        long appId,
        char* fileName,
        int mode
    );

    void (*Delete_File)(
        long appId,
        char* fileName,
        int waitForData,
        int applicationDelete
    );

    void (*Get_File)(
        long appId,
        char* fileName
    );

    void (*Get_Directory)(
        long appId,
        char* dirName
    );

    // COMPSs API Calls
    void (*Barrier)(
        long appId
    );

    void (*BarrierNew)(
        long appId,
        int noMoreTasks
    );

    void (*BarrierGroup)(
        long appId,
        char* groupName,
        char** exceptionMessage
    );

    void (*OpenTaskGroup)(
        char* groupName,
        int implicitBarrier,
        long appId
    );

    void (*CloseTaskGroup)(
        char* groupName,
        long appId
    );

    void (*CancelTaskGroup)(
        char* groupName,
        long appId,
        char** exceptionMessage
    );

    void (*Snapshot)(
        long appId
    );

    int (*GetNumberOfResources)(
        long appId
    );

    void (*RequestResources)(
        long appId,
        int numResources,
        char* groupName
    );

    void (*FreeResources)(
        long appId,
        int numResources,
        char* groupName
    );

    // Misc functions
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

    void (*Get_Object)(
        long appId,
        char* objectId,
        char** buf
    );

    void (*Delete_Object)(
        long appId,
        char* objectId,
        int** buf
    );
    
    void (*Set_wall_clock)(
        long appId,
        long wcl,
        int stopRT
    );

} CompssInterface;

#endif // COMPSS_INTERFACE_H