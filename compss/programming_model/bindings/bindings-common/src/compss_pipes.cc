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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>
#include <limits>
#include <iomanip>

#include "common.h"
#include "compss_pipes.h"
#include "param_metadata.h"
#include "BindingDataManager.h"

using namespace std;

char* command_pipe = NULL;
char* result_pipe = NULL;
FILE* result_pipe_stream;

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

string read_result_from_pipe(){
	if (result_pipe_stream == NULL){
		printf("\n[BINDING-COMMONS] ERROR: Pipe is not set");
	    return NULL;
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


void PIPE_set_pipes(char* comPipe, char* resPipe){
	init_env_vars();
	command_pipe = strdup(comPipe);
	result_pipe = strdup(resPipe);
    result_pipe_stream = fopen(result_pipe , "r");
}

void PIPE_read_command(char** command){
    string buf;
    buf = read_result_from_pipe();
    *command = strdup(buf.c_str());
}
/**
 * Processes the given parameter information. Writse parameter to the stringstream
 */
void process_param(void** params, int i, stringstream& ss) {

	// FORMAT: value(depens on type) Type(int) direction(int) IOstream(int) Prefix(String)
	// Name(String) Content_type(String) Weight(String) Keep_rename(boolean)

	debug_printf("[BINDING-COMMONS] - @process_param - Processing parameter %d\n", i);
    int pv = NUM_FIELDS * i + 0,
        pt = NUM_FIELDS * i + 1,
        pd = NUM_FIELDS * i + 2,
        ps = NUM_FIELDS * i + 3,
        pp = NUM_FIELDS * i + 4,
        pn = NUM_FIELDS * i + 5,
        pc = NUM_FIELDS * i + 6,
        pw = NUM_FIELDS * i + 7,
        pkr = NUM_FIELDS * i + 8;

    void *parVal        =           params[pv];
    int parType         = *(int*)   params[pt];
    int parDirect       = *(int*)   params[pd];
    int parIOStream     = *(int*)   params[ps];
    char *parPrefix     = *(char**) params[pp];
    char *parName       = *(char**) params[pn];
    char *parConType    = *(char**) params[pc];
    char *parWeight	    = *(char**) params[pw];
    int parKeepRename   = *(int*)   params[pkr];

    debug_printf ("[BINDING-COMMONS] - @process_param - NAME: %s\n", parName);

    // Add parameter value
    switch ( (enum datatype) parType) {
        case char_dt:
        case wchar_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Char: %c\n", *(char*)parVal);
            ss << " {  { \"Value\" : \"" << *(char*)parVal << "\", ";
            break;
        case boolean_dt:
        	{
				int _bool = *(int*) parVal;
				if (_bool != 0) {
					debug_printf("[BINDING-COMMONS] - @process_param - Bool: true\n");
                    ss << " { \"Value\" : \"true\", ";
				} else {
					debug_printf("[BINDING-COMMONS] - @process_param - Bool: false\n");
                    ss << " { \"Value\" : \"false\", ";
				}
        	}
            break;
        case short_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Short: %hu\n", *(short*)parVal);
            ss << " { \"Value\" : \"" << *(short*)parVal << "\", ";
            break;
        case int_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Int: %d\n", *(int*)parVal);
            ss << " { \"Value\" : \"" << *(int*)parVal << "\", ";
            break;
        case long_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Long: %ld\n", *(long*)parVal);
            ss << " { \"Value\" : \"" << *(long*)parVal << "\", ";
            break;
        case longlong_dt:
        case float_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Float: %f\n", *(float*)parVal);
            ss << " { \"Value\" : \"" << std::setprecision(std::numeric_limits<float>::digits10 + 1) << *(float*)parVal << "\", ";
            break;
        case double_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Double: %f\n", *(double*)parVal);
            ss << " { \"Value\" : \"" << std::setprecision(std::numeric_limits<long double>::digits10 + 1) << *(double*)parVal << "\", ";
            break;
        case file_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - File: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case directory_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Directory: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case external_stream_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - External Stream: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case external_psco_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Persistent: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case string_dt:
        case string_64_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case binding_object_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Binding Object: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case collection_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Collection: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
				case dict_collection_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Dict Collection: %s\n", *(char **)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case null_dt:
            debug_printf ("[BINDING-COMMONS] - @process_param - Null: NULL\n");
            ss << " { \"Value\" : " << "NULL " << ", ";
            break;
        case void_dt:
        	debug_printf ("[BINDING-COMMONS] - @process_param - void: VOID\n");
        	ss << " { \"Value\" : " << "VOID " << ", ";
        	break;
        case any_dt:
        	debug_printf ("[BINDING-COMMONS] - @process_param - void: ANY\n");
        	ss << " { \"Value\" : " << "ANY " << ", ";
        	break;
        default:
            debug_printf ("[BINDING-COMMONS] - @process_param - The type of the parameter %s is not registered\n", *(char **)parName);
            ss << "ERROR ";
            break;
    }

    // Add parameter type
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DATA_TYPE: %d\n", (enum datatype) parType);
    ss << "\"DataType\" : " << parType << ", ";
    // Add param direction
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DIRECTION: %d\n", (enum direction) parDirect);
    ss << "\"Direction\" : " << parDirect << ", ";


    // Add param stream
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM STD IO STREAM: %d\n", (enum io_stream) parIOStream);
    ss << "\"IOStream\" : " << parIOStream << ", ";


    // Add param prefix
    debug_printf ("[BINDING-COMMONS] - @process_param - PREFIX: %s\n", parPrefix);
    ss << "\"Prefix\" : \"" << parPrefix << "\", ";

    debug_printf ("[BINDING-COMMONS] - @process_param - NAME: %s\n", parName);
    ss << "\"Name\" : \"" << parName << "\", ";

    debug_printf ("[BINDING-COMMONS] - @process_param - CONTENT TYPE: %s\n", parConType);
    ss << "\"ContType\" : \"" << parConType << "\", ";

    debug_printf ("[BINDING-COMMONS] - @process_param - WEIGHT : %s\n", parWeight);
    ss << "\"Weight\" : \"" << parWeight << "\", ";


    if (parKeepRename != 0) {
    	debug_printf ("[BINDING-COMMONS] - @process_param - KEEP RENAME : true\n");
    	ss << "\"KeepRename\" : true }";
    } else {
    	debug_printf ("[BINDING-COMMONS] - @process_param - KEEP RENAME : false\n");
    	ss << "\"KeepRename\" : false }";
    }
}


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

    // End
    debug_printf("[BINDING-COMMONS] - @Off - End\n");
}


void PIPE_Cancel_Application_Tasks(long appId) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_Cancel_Application_Tasks\n");


    // Send CANCEL_APPLICTION_TASKS message and do not wait.
    // MESSAGE: CANCEL_APPLICTION_TASKS appId
    // NO RETURN
    stringstream ss;
    ss << "CANCEL_APPLICATION_TASKS "<< appId << endl;
    write_command_in_pipe(ss);

    debug_printf ("[BINDING-COMMONS] - @PIPE_Cancel_Application_Tasks - Tasks cancelled\n");
}


void PIPE_Get_AppDir(char** buf) {
    debug_printf ("[BINDING-COMMONS] - @PIPE_Get_AppDir - Getting application directory.\n");


    // Call getAppDir expects the path to the app directory.
    // MESSAGE: GET_APPDIR
    // RETURN: appDir path(String)

    stringstream ss;
    ss << "GET_APPDIR" << endl;
    write_command_in_pipe(ss);
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

    stringstream ss;
    ss << "GET_MASTERWORKINGDIR" << endl;
    write_command_in_pipe(ss);
    string result;
    result = read_result_from_pipe();
    // Parse output
    *buf = strdup(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_Get_MasterWorkingDir - directory name: %s\n", *buf);
}


void PIPE_ExecuteTask(long appId, char* className, char* onFailure, int timeout, char* methodName, int priority, int numNodes, int reduce, int reduceChunkSize,
		int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing task execution in bindings-common.\n");

    // Creates message to send and no wait.
    // MESSAGE EXECUTE_TASK METHOD_CLASS className onFailure timeout methodName priority numNodes reduce reduceChunkSize,
    // replicated distributed hasTarget numReturns numParams params[with_format: see process_params]
    // NO RETURN
    stringstream ss;

    ss << "EXECUTE_NESTED_TASK CLASS_METHOD " << className << " " << onFailure << " " << timeout << " " << methodName << " ";


    if (priority != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }

    ss << numNodes << " ";

    if (reduce != 0) {
        ss << "true ";
    } else {
        ss << "false ";
    }

	ss << reduceChunkSize << " ";

	if (replicated != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

	if (distributed != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

    if (hasTarget != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }

    ss << numReturns << " ";

	ss << numParams << " [ ";

    // Create array of parameters
    if(numParams > 0){ //we take out the first iteration in order to put the ',' char between parameters
        debug_printf("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing parameter %d\n", 0);
        process_param(params, 0, ss);
    }
    for (int i = 1; i < numParams; i++) {
    	ss << ", ";
        debug_printf("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing parameter %d\n", i);
        process_param(params, i, ss);
    }
	ss << " ] " << endl;
    // Write execute task method
    write_command_in_pipe(ss);

    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTask - Task processed.\n");
}


void PIPE_ExecuteTaskNew(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes,
		int reduce, int reduceChunkSize, int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTaskNew - Processing task execution in bindings-common. \n");


    // Creates message to send no waits.
    // EXECUTE_TASK SIGNATURE signature onFailure timeout priority numNodes replicated distributed
    // hasTarget numReturns numParams params[with_format: see process_params]
    // NO RETURN

    stringstream ss;
    ss << "EXECUTE_NESTED_TASK SIGNATURE " << signature << " " << onFailure << " " << timeout << " " ;

    if (priority != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

    ss << numNodes << " ";

    if (reduce != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }

    ss << reduceChunkSize << " ";

    if (replicated != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

	if (distributed != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

	if (hasTarget != 0) {
		ss << "true ";
	} else {
		ss << "false ";
	}

	ss << numReturns << " ";

	ss << numParams << " [ ";

    // Create array of parameters
    if(numParams > 0){ //we take out the first iteration in order to put the ',' char between parameters
        debug_printf("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing parameter %d\n", 0);
        process_param(params, 0, ss);
    }
    for (int i = 1; i < numParams; i++) {
    	ss << ", ";
        debug_printf("[BINDING-COMMONS] - @PIPE_ExecuteTask - Processing parameter %d\n", i);
        process_param(params, i, ss);
    }
	ss << " ] " << endl;
	// Write execute task method
	write_command_in_pipe(ss);


    debug_printf ("[BINDING-COMMONS] - @PIPE_ExecuteTaskNew - Task processed.\n");
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

	stringstream ss;
	ss << "REGISTER_CE " << ceSignature << " " << implSignature << " " << implConstraints << " " << implType << " " << implLocal << " " << implIO;

    for (int i = 0; i < 3; i++) {
    	ss << " " << prolog[i];
    }

    for (int i = 0; i < 3; i++) {
    	ss << " " << epilog[i];

    }

    for (int i = 0; i < 3; i++) {
    	ss << " " << container[i];

    }

	ss << " " << numArgs;

    for (int i = 0; i < numArgs; i++) {
    	ss << " " << implTypeArgs[i];

    }

    ss << endl;

    // Write execute task method
    write_command_in_pipe(ss);


    debug_printf("[BINDING-COMMONS] - @PIPE_RegisterCE - Task registered: %s\n", ceSignature);
}

int PIPE_Accessed_File(long appId, char* fileName){
    debug_printf("[BINDING-COMMONS] - @PIPE_Accessed_File - Calling runtime isFileAccessed method  for %s  ...\n", fileName);

    // MESSAGE: FILE_ACCESSED appId filename
    // RETURN: (int) 0 false, otherwise true.
    stringstream ss;
    ss << "FILE_ACCESSED " << appId << " " << fileName << endl;

    write_command_in_pipe(ss);

    // read result
    string result;
	result = read_result_from_pipe();

    // Parse output
    int ret =  atoi(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_Accessed_File - Access to file %s marked as %d\n", fileName, ret);
    return ret;
}

void PIPE_Open_File(long appId, char* fileName, int mode, char** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Open_File - Calling runtime OpenFile method  for %s and mode %d ...\n", fileName, mode);

    // MESSAGE: OPEN_FILE appId fileName mode
    // RETURN: (String) Path of the open file.
    stringstream ss;
    ss << "OPEN_FILE " << appId << " " << fileName << " " << mode << endl;
    write_command_in_pipe(ss);
    string result;
    result = read_result_from_pipe();
    // Parse output
    *buf = strdup(result.c_str());
    *buf=*buf+6;
    debug_printf("[BINDING-COMMONS] - @PIPE_Open_File - COMPSs filename: %s\n", *buf);
}


void PIPE_Close_File(long appId, char* fileName, int mode) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Close_File - Calling runtime closeFile method...\n");

    // MESSAGE CLOSE_FILE appId fileName mode
    // NO RETURN
    stringstream ss;
    ss << "CLOSE_FILE " << appId << " " << fileName << " " << mode << endl;
    write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_Close_File - COMPSs filename: %s\n", fileName);
}


void PIPE_Delete_File(long appId, char* fileName, int wait, int applicationDelete) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - Calling runtime deleteFile method...\n");

    // MESAGE: DELETE_FILE appId fileName
    // RETURN: (int)  0 false, otherwise true
    stringstream ss;
    ss << "DELETE_FILE " << appId << " " << fileName << " ";
    if (wait != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }

    if (applicationDelete != 0) {
        ss << "true ";
    } else {
        ss << "false ";
    }

    write_command_in_pipe(ss);

    string result;
    result = read_result_from_pipe();
    const char* buf = result.c_str();
    buf = buf + 6;
    int res = atoi(buf);

    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - COMPSs filename: %s\n", fileName);
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_File - File erased with status: %i\n", (bool) res);
}


void PIPE_Get_File(long appId, char* fileName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_File - Calling runtime getFile method...\n");

    // MESAGE: GET_FILE appId fileName
    // RETURN: (int)  0 false, otherwise true (Not used, just to wait until file is synchronised at master.)
    stringstream ss;
	ss << "GET_FILE " << appId << " " << fileName << endl;
	write_command_in_pipe(ss);
	string result;
	result = read_result_from_pipe();
	int res = atoi(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_Get_File - COMPSs filename: %s\n", fileName);
}

void PIPE_Get_Directory(long appId, char* dirName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Directory - Calling runtime getDirectory method...\n");
    // MESAGE: GET_DIRECTORY appId dirName
    // RETURN: (int)  0 false, otherwise true (Not used, just to wait until dir is synchronised at master.)
	stringstream ss;
	ss << "GET_DIRECTORY " << appId << " " << dirName << endl;
	write_command_in_pipe(ss);
	string result;
	result = read_result_from_pipe();
	int res = atoi(result.c_str());

	debug_printf("[BINDING-COMMONS] - @PIPE_Get_Directory - COMPSs directory: %s\n", dirName);
}

void PIPE_Get_Object(long appId, char* objectId, char** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Object - Calling runtime getObject method...\n");

    // MESAGE: GET_OBJECT appId objectId/path
    // RETURN: (String)  path/id of the synch object
    stringstream ss;
    ss << "GET_OBJECT" << appId << " " << objectId << endl;
    write_command_in_pipe(ss);
    string result;
    result = read_result_from_pipe();
    *buf = strdup(result.c_str());


    debug_printf("[BINDING-COMMONS] - @PIPE_Get_Object - COMPSs data id: %s\n", *buf);
}


void PIPE_Delete_Object(long appId, char* objectId, int** buf) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_Object - Calling runtime deleteObject method...\n");

    // MESAGE: DELETE_OBJECT appId objectId
    // RETURN: (int)  0 false, otherwise true.
    stringstream ss;
    ss << "DELETE_OBJECT" << appId << " " << objectId << endl;
    write_command_in_pipe(ss);
    string result;
    result = read_result_from_pipe();
    int res = atoi(result.c_str());
    *buf = (int*) &res;

    debug_printf("[BINDING-COMMONS] - @PIPE_Delete_Binding_Object - COMPSs obj: %s\n", objectId);
}


void PIPE_Barrier(long appId) {
	debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - Waiting tasks for APP id: %lu\n", appId);

	// MESSAGE: BARRIER appId
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
	stringstream ss;
	ss << "BARRIER " << appId << endl;
	write_command_in_pipe(ss);
	read_result_from_pipe();

	debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - APP id: %lu\n", appId);
}


void PIPE_BarrierNew(long appId, int noMoreTasks) {
    debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - Waiting tasks for APP id: %lu\n", appId);

	// MESSAGE: BARRIER_NEW appId noMoreTask(boolean)
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
    stringstream ss;
    ss << "BARRIER_NEW " << appId << " ";

    if (noMoreTasks != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }
    ss << endl;
    write_command_in_pipe(ss);
    read_result_from_pipe();

    debug_printf("[BINDING-COMMONS] - @PIPE_Barrier - APP id: %lu\n", appId);
}


void PIPE_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: BARRIER_GROUP appId groupName
    // RETURN: (string) exception message.
    stringstream ss;
    ss << "BARRIER_GROUP " << appId << " " << groupName << endl;
    write_command_in_pipe(ss);
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
            debug_printf("[BINDING-COMMONS] - @PIPE_BarrierGroup - Unexpected command %s to release group: %s\n", groupName);
        }
    }
}


void PIPE_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
    debug_printf("[BINDING-COMMONS] - @PIPE_OpenTaskGroup - Opening task group %s ...\n", groupName);

    // MESSAGE: OPEN_TASK_GROUP appId groupName implicitBarrier(boolean)
    // NO RETURN
    stringstream ss;
    ss << "OPEN_TASK_GROUP " << appId << " " << groupName << " ";
    if (implicitBarrier != 0) {
    	ss << "true ";
    } else {
    	ss << "false ";
    }
    ss << endl;
    write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_OpenTaskGroup - COMPSs group name: %s\n", groupName);
}


void PIPE_CloseTaskGroup(char* groupName, long appId){
    debug_printf("[BINDING-COMMONS] - @PIPE_CloseTaskGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: CLOSE_TASK_GROUP appId groupName
    // NO RETURN
    stringstream ss;
    ss << "CLOSE_TASK_GROUP " << appId << " " << groupName << endl;
    write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_CloseTaskGroup - Task group %s closed.\n", groupName);
}

void PIPE_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
    debug_printf("[BINDING-COMMONS] - @PIPE_CancelTaskGroup - COMPSs group name: %s\n", groupName);

    // MESSAGE: CANCEL_TASK_GROUP appId groupName
    // NO RETURN
    stringstream ss;
    ss << "CANCEL_TASK_GROUP " << appId << " " << groupName << endl;
    write_command_in_pipe(ss);
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
					groupName);
		}
	}

    debug_printf("[BINDING-COMMONS] - @PIPE_ClancelTaskGroup - Task group %s closed.\n", groupName);
}


void PIPE_Snapshot(long appId) {
	debug_printf("[BINDING-COMMONS] - @PIPE_Snapshot - Snapshot for APP id: %lu\n", appId);

	// MESSAGE: SNAPSHOT appId
	// RETURNS: Whatever string (Not used, just to wait until dir is synchronised at master.)
	stringstream ss;
	ss << "SNAPSHOT " << appId << endl;
	write_command_in_pipe(ss);
	string result;
	result = read_result_from_pipe();

	debug_printf("[BINDING-COMMONS] - @PIPE_Snapshot - APP id: %lu\n", appId);
}


void PIPE_EmitEvent(int type, long id) {
    debug_printf("[BINDING-COMMONS] - @PIPE_EmitEvent - Emit Event\n");

    // Check validity
    if (type < 0  or id < 0) {
        debug_printf ("[BINDING-COMMONS] - @PIPE_EmitEvent - Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);
    }
    // MESSAGE: EMIT_EVENT type id
    // NO RETURN
    stringstream ss;
    ss << "EMIT_EVENT " << type << " " << id << endl;
    write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_EmitEvent - Event emitted\n");
}


int PIPE_GetNumberOfResources(long appId) {
    debug_printf("[BINDING-COMMONS] - @PIPE_GetNumberOfResources - Requesting number of resources\n");

    // MESSAGE: GET_RESOURCES appId
    // RETURN: (int) number of resources
    stringstream ss;
    ss << "GET_RESOURCES " << appId << endl;
    write_command_in_pipe(ss);
    string result;
    result = read_result_from_pipe();
    int resources = atoi(result.c_str());

    debug_printf("[BINDING-COMMONS] - @PIPE_GetNumberOfResources - Number of active resources %u\n", (int) resources);
    return (int) resources;
}

void PIPE_RequestResources(long appId, int numResources, char* groupName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_RequestResources - Requesting resources for APP id: %lu\n", appId);
    debug_printf("[BINDING-COMMONS] - @PIPE_RequestResources - numResources: %u\n", numResources);
    debug_printf("[BINDING-COMMONS] - @PIPE_RequestResources - groupName: %s\n", groupName);

    // MESSAGE: REQUEST_RESOURCES appId numResources char*groupName
    // NO RETURN
	stringstream ss;
	ss << "REQUEST_RESOURCES " << appId << " " << numResources << "" << groupName << endl;
	write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_RequestResources - Resources creation requested");
}

void PIPE_FreeResources(long appId, int numResources, char* groupName) {
    debug_printf("[BINDING-COMMONS] - @PIPE_FreeResources - Freeing resources for APP id: %lu\n", appId);
    debug_printf("[BINDING-COMMONS] - @PIPE_FreeResources - numResources: %u\n", numResources);
    debug_printf("[BINDING-COMMONS] - @PIPE_FreeResources - groupName: %s\n", groupName);

    // MESSAGE: FREE_RESOURCES appId numResources groupName
    // NO RETURN
    stringstream ss;
	ss << "FREE_RESOURCES " << appId << " " << numResources << " " << groupName << endl;
	write_command_in_pipe(ss);

    debug_printf("[BINDING-COMMONS] - @PIPE_FreeResources - Resources destruction requested");
}

void PIPE_set_wall_clock(long appId, long wcl, int stopRT){
	debug_printf ("[BINDING-COMMONS] - @PIPE_set_wall_clock NOT CURRENTLY IMPLEMENTED FOR PIPES\n");
}
