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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>
#include <fstream>

#include "common.h"
#include "compss_interface.h"
#include "compss_pipes.h"
#include "command_builders.h"
#include "param_metadata.h"

using namespace std;

char* command_pipe = NULL;
char* result_pipe = NULL;
FILE* result_pipe_stream;

namespace {
    const char* response_payload(const std::string& result) {
        const char* data = result.c_str();
        if (result.size() >= 6 &&
            (strncmp(data, "SYNCH ", 6) == 0)) {
            data += 6;
            while (*data == ' ') {
                ++data;
            }
        }
        return data;
    }
}

// ******************************
// Private helper methods
// ******************************

void write_command_in_pipe(stringstream& ss){
	    string myString = ss.str();
	    ofstream ofs;
	    if (command_pipe == NULL){
	    	printf("\n[BINDING-COMMONS] ERROR: Pipe is not set");
	    	return;
	    }
	    ofs.open(command_pipe,ios_base::app);
	    ofs << ss.str();
	    ofs.close();
}

void write_command_in_pipe(const string& command) {
    stringstream ss;
    ss << command;
    write_command_in_pipe(ss);
}

string read_result_from_pipe(){
	if (result_pipe_stream == NULL){
		printf("\n[BINDING-COMMONS] ERROR: Pipe is not set");
	    return string();
	}

	char buf[BUFSIZ];
	std::stringstream oss;
	while (1) {
		if( fgets (buf, BUFSIZ, result_pipe_stream) != NULL ) {
			int buflen = strlen(buf);
			if (buflen >0){
             	if (buf[buflen-1] == '\n'){
                        buf[buflen-1] = '\0';
                        oss << buf;
				    	return oss.str();
				} else {
                    oss << buf;
                    // line was truncated. Read another block to complete line.
                }
			}
		} else {
			// Necessary to avoid that fgets return NULL after closing the pipe for the first time.
			clearerr(result_pipe_stream);
		}
	}
}

void PIPE_read_command(char** command){
    string buf;
    buf = read_result_from_pipe();
    *command = strdup(buf.c_str());
}

// ******************************
// Workflow methods
// ******************************
long PIPE_WF_getId(CompssWorkflow* self) {
    return 0;
}

void PIPE_WF_deregister(CompssWorkflow* self) {
}

void PIPE_WF_openTaskGroup(CompssWorkflow* self, const char* groupName, bool implicitBarrier) {
    debug_printf("[BINDING-COMMONS] - @PIPE_OpenTaskGroup - Opening task group %s ...\n", groupName);

    // MESSAGE: OPEN_TASK_GROUP groupName implicitBarrier(boolean)
    // NO RETURN
    write_command_in_pipe(build_open_task_group_command(groupName, implicitBarrier));

    debug_printf("[BINDING-COMMONS] - @PIPE_OpenTaskGroup - COMPSs group name: %s\n", groupName);
}

void PIPE_WF_closeTaskGroup(CompssWorkflow* self, const char* groupName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_CloseTaskGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: CLOSE_TASK_GROUP groupName
    // NO RETURN
    write_command_in_pipe(build_close_task_group_command(groupName));

    debug_printf("[BINDING-COMMONS] - @PIPE_CloseTaskGroup - Task group %s closed.\n", groupName);
}

void PIPE_WF_executeTask(CompssWorkflow* self, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize,
                        int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing task execution in bindings-common. \n");

    // Creates message to send no waits.
    // EXECUTE_TASK signature onFailure timeout priority numNodes replicated distributed
    // hasTarget numReturns numParams params[with_format: see process_params]
    // NO RETURN

    write_command_in_pipe(build_execute_task_command(signature,
                                                               onFailure,
                                                               timeout,
                                                               priority,
                                                               numNodes,
                                                               reduce,
                                                               reduceChunkSize,
                                                               replicated,
                                                               distributed,
                                                               hasTarget,
                                                               numReturns,
                                                               numParams,
                                                               params));


    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTask - Task processed.\n");
}

void PIPE_WF_executeHttpTask(CompssWorkflow* self, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce,
                         int reduceChunkSize, int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteHttpTask - HTTP task execution in bindings-common. \n");
    debug_printf ("[BINDING-COMMONS] NOT YET IMPLEMENTED")
}

void PIPE_WF_cancelTaskGroup(CompssWorkflow* self, const char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @PIPE_CancelTaskGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: CANCEL_TASK_GROUP groupName
    // NO RETURN
    write_command_in_pipe(build_cancel_task_group_command(groupName));
	string result;
	bool barrier_finished = false;
	while (!barrier_finished) {
		result = read_result_from_pipe();
		const char* buf = result.c_str();
		if (strncmp(buf, "COMPSS_EXCEPTION", 16) == 0) {
			buf = buf + 22;
			*exceptionMessage = strdup(buf);
			barrier_finished = true;
			debug_printf(
					"[BINDING-COMMONS] - @PIPE_BarrierGroup - Barrier ended for COMPSs group name: %s with an exception\n",
					groupName);
		} else if (strncmp(buf, "SYNCH", 5) == 0) {
			debug_printf(
					"[BINDING-COMMONS] - @PIPE_BarrierGroup - Barrier ended for COMPSs group name: %s\n",
					groupName);
			barrier_finished = true;
		} else {
			debug_printf(
					"[BINDING-COMMONS] - @PIPE_BarrierGroup - Unexpected command %s to release group: %s\n",
					buf, groupName);
		}
	}

    debug_printf("[BINDING-COMMONS] - @PIPE_ClancelTaskGroup - Task group %s closed.\n", groupName);
}

void PIPE_WF_cancelApplicationTasks(CompssWorkflow* self) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_Cancel_Application_Tasks\n");


    // Send CANCEL_APPLICTION_TASKS message and do not wait.
    // MESSAGE: CANCEL_APPLICTION_TASKS
    // NO RETURN
    write_command_in_pipe(build_cancel_application_tasks_command());

    debug_printf ("[BINDING-COMMONS] - @PIPE_Cancel_Application_Tasks - Tasks cancelled\n");

}

void PIPE_WF_barrier(CompssWorkflow* self) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - Waiting tasks\n");

	// MESSAGE: BARRIER
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
    write_command_in_pipe(build_barrier_command());
	read_result_from_pipe();

	debug_printf("[BINDING-COMMONS] - @PIPE_Barrier\n");
}

void PIPE_WF_barrierWithFlag(CompssWorkflow* self, bool noMoreTasksFlag) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - Waiting tasks\n");

	// MESSAGE: BARRIER_NEW noMoreTask(boolean)
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
    write_command_in_pipe(build_barrier_new_command(noMoreTasksFlag));
    read_result_from_pipe();

    debug_printf("[BINDING-COMMONS] - @PIPE_Barrier\n");
}

void PIPE_WF_barrierGroup(CompssWorkflow* self, const char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: BARRIER_GROUP groupName
    // RETURN: (string) exception message.
    write_command_in_pipe(build_barrier_group_command(groupName));
    string result;
    bool barrier_finished = false;
    while (!barrier_finished) {
        result = read_result_from_pipe();
        const char* buf = result.c_str();
        if(strncmp(buf, "COMPSS_EXCEPTION", 16) == 0){
            buf = buf + 22;
            *exceptionMessage = strdup(buf);
            barrier_finished = true;
            debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - Barrier ended for COMPSs group name: %s with an exception\n", groupName);
        } else if(strncmp(buf, "SYNCH", 5) == 0){
            debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - Barrier ended for COMPSs group name: %s\n", groupName);
            barrier_finished = true;
        } else{
            debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - Unexpected command %s to release group: %s\n", buf, groupName);
        }
    }
}

void PIPE_WF_noMoreTasks(CompssWorkflow* self) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_noMoreTasks. \n");
    debug_printf ("[BINDING-COMMONS] NOT YET IMPLEMENTED")
}


int PIPE_WF_isFileAccessed(CompssWorkflow* self, char* fileName){
    debug_printf("[BINDING-COMMONS] - @PIPE_Accessed_File - Calling runtime isFileAccessed method  for %s  ...\n", fileName);

    // MESSAGE: FILE_ACCESSED filename
    // RETURN: (int) 0 false, otherwise true.
    write_command_in_pipe(build_file_accessed_command(fileName));

    // read result
    string result = read_result_from_pipe();
    const char* response = response_payload(result);
    int ret = atoi(response);

    debug_printf("[BINDING-COMMONS] - @PIPE_Accessed_File - Access to file %s marked as %d\n", fileName, ret);
    return ret;
}


void PIPE_WF_openFile(CompssWorkflow* self, char* fileName, int mode, char** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Open_File - Calling runtime OpenFile method  for %s and mode %d ...\n", fileName, mode);

    // MESSAGE: OPEN_FILE fileName mode
    // RETURN: (String) Path of the open file.
    write_command_in_pipe(build_open_file_command(fileName, mode));
    string result = read_result_from_pipe();
    const char* response = response_payload(result);
    // Parse output
    *buf = strdup(response);
    debug_printf("[BINDING-COMMONS] - @PIPE_Open_File - COMPSs filename: %s\n", *buf);
}


void PIPE_WF_getFile(CompssWorkflow* self, char* fileName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_File - Calling runtime getFile method...\n");

    // MESAGE: GET_FILE fileName
    // RETURN: (int)  0 false, otherwise true (Not used, just to wait until file is synchronised at master.)
    write_command_in_pipe(build_get_file_command(fileName));
	string result = read_result_from_pipe();
    const char* response = response_payload(result);
	int res = atoi(response);

    debug_printf("[BINDING-COMMONS] - @PIPE_Get_File - COMPSs filename: %s\n", fileName);
}


void PIPE_WF_closeFile(CompssWorkflow* self, char* fileName, int mode) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Close_File - Calling runtime closeFile method...\n");

    // MESSAGE CLOSE_FILE fileName mode
    // NO RETURN
    write_command_in_pipe(build_close_file_command(fileName, mode));

    debug_printf("[BINDING-COMMONS] - @PIPE_Close_File - COMPSs filename: %s\n", fileName);
}


bool PIPE_WF_deleteFile(CompssWorkflow* self, char* fileName, int wait, int applicationDelete) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - Calling runtime deleteFile method...\n");

    // MESAGE: DELETE_FILE fileName
    // RETURN: (int)  0 false, otherwise true
    write_command_in_pipe(build_delete_file_command(fileName, wait, applicationDelete));

    string result = read_result_from_pipe();
    const char* response = response_payload(result);
    int res = atoi(response);

    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - COMPSs filename: %s\n", fileName);
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - File erased with status: %i\n", (bool) res);
    return (bool)res;
}


void PIPE_WF_getDirectory(CompssWorkflow* self, char* dirName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Directory - Calling runtime getDirectory method...\n");
    // MESAGE: GET_DIRECTORY dirName
    // RETURN: (int)  0 false, otherwise true (Not used, just to wait until dir is synchronised at master.)
    write_command_in_pipe(build_get_directory_command(dirName));
	string result = read_result_from_pipe();
    const char* response = response_payload(result);
	int res = atoi(response);

	debug_printf("[BINDING-COMMONS] - @PIPE_Get_Directory - COMPSs directory: %s\n", dirName);
}


void PIPE_WF_getObject(CompssWorkflow* self, char* objectId, char** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Object - Calling runtime getObject method...\n");

    // MESAGE: GET_OBJECT objectId/path
    // RETURN: (String)  path/id of the synch object
    write_command_in_pipe(build_get_object_command(objectId));
    string result;
    result = read_result_from_pipe();
    *buf = strdup(result.c_str());


    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Object - COMPSs data id: %s\n", *buf);
}


void PIPE_WF_deleteObject(CompssWorkflow* self, char* objectId, int** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_Object - Calling runtime deleteObject method...\n");

    // MESAGE: DELETE_OBJECT objectId
    // RETURN: (int)  0 false, otherwise true.
    write_command_in_pipe(build_delete_object_command(objectId));
    string result = read_result_from_pipe();
    const char* response = response_payload(result);
    int res = atoi(response);
    int* heap_value = static_cast<int*>(malloc(sizeof(int)));
    if (heap_value == NULL) {
        print_error("[BINDING-COMMONS] - @PIPE_Delete_Object - Allocation failure\n");
        *buf = NULL;
    } else {
        *heap_value = res;
        *buf = heap_value;
    }

    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_Binding_Object - COMPSs obj: %s\n", objectId);
}


void PIPE_WF_snapshot(CompssWorkflow* self) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Snapshot - Snapshot\n");

	// MESSAGE: SNAPSHOT
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
    write_command_in_pipe(build_snapshot_command());
	string result;
	result = read_result_from_pipe();

	debug_printf("[BINDING-COMMONS] - @PIPE_Snapshot\n");
}



CompssWorkflow* PIPE_RegisterWorkflow() {
    debug_printf("[BINDING-COMMONS] - @PIPE_RegisterWorkflow\n");

    // Currently pipes already register previous Workflow beforehand.
    CompssWorkflow* wf = (CompssWorkflow*) malloc(sizeof(CompssWorkflow));
    
    wf->getId = PIPE_WF_getId;
    wf->deregister = PIPE_WF_deregister;
    wf->openTaskGroup = PIPE_WF_openTaskGroup;
    wf->closeTaskGroup = PIPE_WF_closeTaskGroup;
    wf->executeTask = PIPE_WF_executeTask;
    wf->executeHttpTask = PIPE_WF_executeHttpTask;
    wf->cancelTaskGroup = PIPE_WF_cancelTaskGroup;
    wf->cancelApplicationTasks = PIPE_WF_cancelApplicationTasks;
    wf->noMoreTasks = PIPE_WF_noMoreTasks;
    wf->barrier = PIPE_WF_barrier;
    wf->barrierWithFlag = PIPE_WF_barrierWithFlag;
    wf->barrierGroup = PIPE_WF_barrierGroup;
    wf->snapshot = PIPE_WF_snapshot;
    wf->get_object = PIPE_WF_getObject;
    wf->delete_object = PIPE_WF_deleteObject;
    wf->is_file_accessed = PIPE_WF_isFileAccessed;
    wf->open_file = PIPE_WF_openFile;
    wf->get_file = PIPE_WF_getFile;
    wf->close_file = PIPE_WF_closeFile;
    wf->delete_file = PIPE_WF_deleteFile;
    wf->get_directory = PIPE_WF_getDirectory;

    debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterWorkflow - Workflow registered\n");
    return wf;
}

// ******************************
// API functions
// ******************************

void PIPE_On() {
    debug_printf ("[BINDING-COMMONS] - @PIPE_On\n");
    init_env_vars();
    // TODO: I think nothing is required in current case
    debug_printf ("[BINDING-COMMONS] - @PIPE_On NOT CURRENTLY IMPLEMENTED FOR PIPES\n");
    // Create runtime
    // Call startIT
}


void PIPE_Off(int code) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Off\n");
    // TODO: I think nothing is required in current case
    debug_printf ("[BINDING-COMMONS] - @PIPE_Off NOT CURRENTLY IMPLEMENTED FOR PIPES\n");

    // Call noMoreTasks

    // Call stopIT

    // TODO: close resources

    // End
    debug_printf("[BINDING-COMMONS] - @Off - End\n");
}


void PIPE_Get_AppDir(char** buf) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_Get_AppDir - Getting application directory.\n");


    // Call getAppDir expects the path to the app directory.
    // MESSAGE: GET_APPDIR
    // RETURN: appDir path(String)

    write_command_in_pipe(build_get_app_dir_command());
    string result;
    result = read_result_from_pipe();
    // Parse output
    *buf = strdup(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_Get_AppDir - directory name: %s\n", *buf);
}


void PIPE_Get_MasterWorkingDir(char** buf) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_Get_MasterWorkingDir - Getting master working directory (tmp).\n");


    // Call getMasterWorkingDir expects the path to the app directory.
    // MESSAGE: GET_MASTERWORKINGDIR
    // RETURN: masterWorkingDir path(String)

    write_command_in_pipe(build_get_master_working_dir_command());
    string result;
    result = read_result_from_pipe();
    // Parse output
    *buf = strdup(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_Get_MasterWorkingDir - directory name: %s\n", *buf);
}


void PIPE_RegisterCE(char* ceSignature, char* implSignature, char* implConstraints, char* implType, char* implLocal, char* implIO, char** prolog, char** epilog, char** container, int numArgs, char** implTypeArgs) {
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - ceSignature:     %s\n", ceSignature);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - implSignature:   %s\n", implSignature);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - implConstraints: %s\n", implConstraints);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - implType:        %s\n", implType);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - implLocal:        %s\n", implLocal);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - implIO:        %s\n", implIO);
    //debug_printf ("[BINDING-COMMONS] - @PIPE_RegisterCE - numParams:      %d\n", numParams);


	// Creates message to send and no wait.
	// REGISTER_CE ceSignature implSignature implConstraints implType implIO numArgs implTypeArgs[]
	// NO RETURN

    write_command_in_pipe(build_register_ce_command(ceSignature,
                                                    implSignature,
                                                    implConstraints,
                                                    implType,
                                                    implLocal,
                                                    implIO,
                                                    prolog,
                                                    epilog,
                                                    container,
                                                    numArgs,
                                                    implTypeArgs));


    debug_printf("[BINDING-COMMONS] - @PIPE_RegisterCE - Task registered: %s\n", ceSignature);
}

void PIPE_EmitEvent(int type, long id) {
    debug_printf("[BINDING-COMMONS] - @PIPE_EmitEvent - Emit Event\n");

    // Check validity
    if (type < 0  or id < 0) {
        debug_printf ("[BINDING-COMMONS] - @PIPE_EmitEvent - Error: event type and ID must be positive integers, but found: type: %d, ID: %ld\n", type, id);
    }
    // MESSAGE: EMIT_EVENT type id
    // NO RETURN
    write_command_in_pipe(build_emit_event_command(type, id));

    debug_printf("[BINDING-COMMONS] - @PIPE_EmitEvent - Event emitted\n");
}

void PIPE_set_wall_clock(long appId, long wcl, int stopRT){
	debug_printf("[BINDING-COMMONS] - @PIPE_set_wall_clock NOT CURRENTLY IMPLEMENTED FOR PIPES\n");
}

CompssInterface setup_PIPE_runtime(char* comPipe, char* resPipe){
	init_env_vars();
	command_pipe = strdup(comPipe);
	result_pipe = strdup(resPipe);
    result_pipe_stream = fopen(result_pipe , "r");

    CompssInterface iface{};
    iface.On = PIPE_On;
    iface.Off = PIPE_Off;
    iface.read_command = PIPE_read_command;

    iface.Get_AppDir = PIPE_Get_AppDir;
    iface.Get_MasterWorkingDir = PIPE_Get_MasterWorkingDir;
    
    iface.RegisterCE = PIPE_RegisterCE;
    iface.registerWorkflow = PIPE_RegisterWorkflow;
    
    iface.EmitEvent = PIPE_EmitEvent;

    iface.Set_wall_clock = PIPE_set_wall_clock;

    return iface;
}
