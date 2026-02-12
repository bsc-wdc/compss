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
#include "command_builders.h"

#include "common.h"
#include "param_metadata.h"

#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

namespace {

const char* to_bool_literal(int value) {
    return value != 0 ? "true" : "false";
}

std::string build_param_fragment(void** params, int index) {
    debug_printf("[BINDING-COMMONS] - @process_param - Processing parameter %d\n", index);

    int pv = NUM_FIELDS * index + 0,
        pt = NUM_FIELDS * index + 1,
        pd = NUM_FIELDS * index + 2,
        ps = NUM_FIELDS * index + 3,
        pp = NUM_FIELDS * index + 4,
        pn = NUM_FIELDS * index + 5,
        pc = NUM_FIELDS * index + 6,
        pw = NUM_FIELDS * index + 7,
        pkr = NUM_FIELDS * index + 8;

    void* parVal = params[pv];
    int parType = *(int*)params[pt];
    int parDirect = *(int*)params[pd];
    int parIOStream = *(int*)params[ps];
    char* parPrefix = *(char**)params[pp];
    char* parName = *(char**)params[pn];
    char* parConType = *(char**)params[pc];
    char* parWeight = *(char**)params[pw];
    int parKeepRename = *(int*)params[pkr];

    debug_printf("[BINDING-COMMONS] - @process_param - NAME: %s\n", parName);

    std::ostringstream ss;

    switch ((enum datatype)parType) {
        case char_dt:
        case wchar_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Char: %c\n", *(char*)parVal);
            ss << " {  { \"Value\" : \"" << *(char*)parVal << "\", ";
            break;
        case boolean_dt: {
            int flag = *(int*)parVal;
            if (flag != 0) {
                debug_printf("[BINDING-COMMONS] - @process_param - Bool: true\n");
                ss << " { \"Value\" : \"true\", ";
            } else {
                debug_printf("[BINDING-COMMONS] - @process_param - Bool: false\n");
                ss << " { \"Value\" : \"false\", ";
            }
            break;
        }
        case short_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Short: %hu\n", *(short*)parVal);
            ss << " { \"Value\" : \"" << *(short*)parVal << "\", ";
            break;
        case int_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Int: %d\n", *(int*)parVal);
            ss << " { \"Value\" : \"" << *(int*)parVal << "\", ";
            break;
        case long_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Long: %ld\n", *(long*)parVal);
            ss << " { \"Value\" : \"" << *(long*)parVal << "\", ";
            break;
        case longlong_dt: {
            long long value = *(long long*)parVal;
            debug_printf("[BINDING-COMMONS] - @process_param - Long Long: %lld\n", value);
            ss << " { \"Value\" : \"" << value << "\", ";
            break;
        }
        case float_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Float: %f\n", *(float*)parVal);
            ss << " { \"Value\" : \"" << std::setprecision(std::numeric_limits<float>::digits10 + 1)
               << *(float*)parVal << "\", ";
            break;
        case double_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Double: %f\n", *(double*)parVal);
            ss << " { \"Value\" : \"" << std::setprecision(std::numeric_limits<long double>::digits10 + 1)
               << *(double*)parVal << "\", ";
            break;
        case file_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - File: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case directory_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Directory: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case external_stream_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - External Stream: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case external_psco_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Persistent: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case string_dt:
        case string_64_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - String: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case binding_object_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Binding Object: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case collection_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Collection: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case dict_collection_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Dict Collection: %s\n", *(char**)parVal);
            ss << " { \"Value\" : \"" << *(char**)parVal << "\", ";
            break;
        case null_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - Null: NULL\n");
            ss << " { \"Value\" : " << "NULL " << ", ";
            break;
        case void_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - void: VOID\n");
            ss << " { \"Value\" : " << "VOID " << ", ";
            break;
        case any_dt:
            debug_printf("[BINDING-COMMONS] - @process_param - void: ANY\n");
            ss << " { \"Value\" : " << "ANY " << ", ";
            break;
        default:
            debug_printf("[BINDING-COMMONS] - @process_param - The type of the parameter %s is not registered\n",
                         parName);
            ss << "ERROR ";
            break;
    }

    debug_printf("[BINDING-COMMONS] - @process_param - ENUM DATA_TYPE: %d\n", (enum datatype)parType);
    ss << "\"DataType\" : " << parType << ", ";
    debug_printf("[BINDING-COMMONS] - @process_param - ENUM DIRECTION: %d\n", (enum direction)parDirect);
    ss << "\"Direction\" : " << parDirect << ", ";
    debug_printf("[BINDING-COMMONS] - @process_param - ENUM STD IO STREAM: %d\n", (enum io_stream)parIOStream);
    ss << "\"IOStream\" : " << parIOStream << ", ";
    debug_printf("[BINDING-COMMONS] - @process_param - PREFIX: %s\n", parPrefix);
    ss << "\"Prefix\" : \"" << parPrefix << "\", ";
    debug_printf("[BINDING-COMMONS] - @process_param - NAME: %s\n", parName);
    ss << "\"Name\" : \"" << parName << "\", ";
    debug_printf("[BINDING-COMMONS] - @process_param - CONTENT TYPE: %s\n", parConType);
    ss << "\"ContType\" : \"" << parConType << "\", ";
    debug_printf("[BINDING-COMMONS] - @process_param - WEIGHT : %s\n", parWeight);
    ss << "\"Weight\" : \"" << parWeight << "\", ";

    if (parKeepRename != 0) {
        debug_printf("[BINDING-COMMONS] - @process_param - KEEP RENAME : true\n");
        ss << "\"KeepRename\" : true }";
    } else {
        debug_printf("[BINDING-COMMONS] - @process_param - KEEP RENAME : false\n");
        ss << "\"KeepRename\" : false }";
    }

    return ss.str();
}

}  // namespace

std::string build_cancel_application_tasks_command(long appId) {
    std::ostringstream oss;
    oss << "CANCEL_APPLICATION_TASKS " << appId << std::endl;
    return oss.str();
}

std::string build_get_app_dir_command() {
    std::ostringstream oss;
    oss << "GET_APPDIR" << std::endl;
    return oss.str();
}

std::string build_get_master_working_dir_command() {
    std::ostringstream oss;
    oss << "GET_MASTERWORKINGDIR" << std::endl;
    return oss.str();
}

std::string build_execute_task_class_command(const char* className,
                                             const char* onFailure,
                                             int timeout,
                                             const char* methodName,
                                             int priority,
                                             int numNodes,
                                             int reduce,
                                             int reduceChunkSize,
                                             int replicated,
                                             int distributed,
                                             int hasTarget,
                                             int numReturns,
                                             int numParams,
                                             void** params) {
    std::ostringstream ss;
    ss << "EXECUTE_NESTED_TASK CLASS_METHOD " << className << " " << onFailure << " " << timeout << " "
       << methodName << " " << to_bool_literal(priority) << " " << numNodes << " " << to_bool_literal(reduce) << " "
       << reduceChunkSize << " " << to_bool_literal(replicated) << " " << to_bool_literal(distributed) << " "
       << to_bool_literal(hasTarget) << " " << numReturns << " " << numParams << " [ ";

    if (numParams > 0) {
        ss << build_param_fragment(params, 0);
        for (int i = 1; i < numParams; ++i) {
            ss << ", " << build_param_fragment(params, i);
        }
    }

    ss << " ] " << std::endl;
    return ss.str();
}

std::string build_execute_task_signature_command(const char* signature,
                                                  const char* onFailure,
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
                                                  void** params) {
    std::ostringstream ss;
    ss << "EXECUTE_NESTED_TASK SIGNATURE " << signature << " " << onFailure << " " << timeout << " "
       << to_bool_literal(priority) << " " << numNodes << " " << to_bool_literal(reduce) << " " << reduceChunkSize
       << " " << to_bool_literal(replicated) << " " << to_bool_literal(distributed) << " " << to_bool_literal(hasTarget)
       << " " << numReturns << " " << numParams << " [ ";

    if (numParams > 0) {
        ss << build_param_fragment(params, 0);
        for (int i = 1; i < numParams; ++i) {
            ss << ", " << build_param_fragment(params, i);
        }
    }

    ss << " ] " << std::endl;
    return ss.str();
}

std::string build_register_ce_command(const char* ceSignature,
                                      const char* implSignature,
                                      const char* implConstraints,
                                      const char* implType,
                                      const char* implLocal,
                                      const char* implIO,
                                      char** prolog,
                                      char** epilog,
                                      char** container,
                                      int numArgs,
                                      char** implTypeArgs) {
    std::ostringstream ss;
    ss << "REGISTER_CE " << ceSignature << " " << implSignature << " " << implConstraints << " " << implType << " "
       << implLocal << " " << implIO;

    for (int i = 0; i < 3; ++i) {
        ss << " " << prolog[i];
    }
    for (int i = 0; i < 3; ++i) {
        ss << " " << epilog[i];
    }
    for (int i = 0; i < 3; ++i) {
        ss << " " << container[i];
    }

    ss << " " << numArgs;
    for (int i = 0; i < numArgs; ++i) {
        ss << " " << implTypeArgs[i];
    }

    ss << std::endl;
    return ss.str();
}

std::string build_file_accessed_command(long appId, const char* fileName) {
    std::ostringstream ss;
    ss << "FILE_ACCESSED " << appId << " " << fileName << std::endl;
    return ss.str();
}

std::string build_open_file_command(long appId, const char* fileName, int mode) {
    std::ostringstream ss;
    ss << "OPEN_FILE " << appId << " " << fileName << " " << mode << std::endl;
    return ss.str();
}

std::string build_close_file_command(long appId, const char* fileName, int mode) {
    std::ostringstream ss;
    ss << "CLOSE_FILE " << appId << " " << fileName << " " << mode << std::endl;
    return ss.str();
}

std::string build_delete_file_command(long appId, const char* fileName, int wait, int applicationDelete) {
    std::ostringstream ss;
    ss << "DELETE_FILE " << appId << " " << fileName << " " << to_bool_literal(wait) << " "
       << to_bool_literal(applicationDelete) << std::endl;
    return ss.str();
}

std::string build_get_file_command(long appId, const char* fileName) {
    std::ostringstream ss;
    ss << "GET_FILE " << appId << " " << fileName << std::endl;
    return ss.str();
}

std::string build_get_directory_command(long appId, const char* dirName) {
    std::ostringstream ss;
    ss << "GET_DIRECTORY " << appId << " " << dirName << std::endl;
    return ss.str();
}

std::string build_get_object_command(long appId, const char* objectId) {
    std::ostringstream ss;
    ss << "GET_OBJECT" << appId << " " << objectId << std::endl;
    return ss.str();
}

std::string build_delete_object_command(long appId, const char* objectId) {
    std::ostringstream ss;
    ss << "DELETE_OBJECT" << appId << " " << objectId << std::endl;
    return ss.str();
}

std::string build_barrier_command(long appId) {
    std::ostringstream ss;
    ss << "BARRIER " << appId << std::endl;
    return ss.str();
}

std::string build_barrier_new_command(long appId, int noMoreTasks) {
    std::ostringstream ss;
    ss << "BARRIER_NEW " << appId << " " << to_bool_literal(noMoreTasks) << " " << std::endl;
    return ss.str();
}

std::string build_barrier_group_command(long appId, const char* groupName) {
    std::ostringstream ss;
    ss << "BARRIER_GROUP " << appId << " " << groupName << std::endl;
    return ss.str();
}

std::string build_open_task_group_command(long appId, const char* groupName, int implicitBarrier) {
    std::ostringstream ss;
    ss << "OPEN_TASK_GROUP " << appId << " " << groupName << " " << to_bool_literal(implicitBarrier) << " "
       << std::endl;
    return ss.str();
}

std::string build_close_task_group_command(long appId, const char* groupName) {
    std::ostringstream ss;
    ss << "CLOSE_TASK_GROUP " << appId << " " << groupName << std::endl;
    return ss.str();
}

std::string build_cancel_task_group_command(long appId, const char* groupName) {
    std::ostringstream ss;
    ss << "CANCEL_TASK_GROUP " << appId << " " << groupName << std::endl;
    return ss.str();
}

std::string build_snapshot_command(long appId) {
    std::ostringstream ss;
    ss << "SNAPSHOT " << appId << std::endl;
    return ss.str();
}

std::string build_emit_event_command(int type, long id) {
    std::ostringstream ss;
    ss << "EMIT_EVENT " << type << " " << id << std::endl;
    return ss.str();
}

std::string build_get_number_of_resources_command(long appId) {
    std::ostringstream ss;
    ss << "GET_RESOURCES " << appId << std::endl;
    return ss.str();
}

std::string build_request_resources_command(long appId, int numResources, const char* groupName) {
    std::ostringstream ss;
    ss << "REQUEST_RESOURCES " << appId << " " << numResources << " " << groupName << std::endl;
    return ss.str();
}

std::string build_free_resources_command(long appId, int numResources, const char* groupName) {
    std::ostringstream ss;
    ss << "FREE_RESOURCES " << appId << " " << numResources << " " << groupName << std::endl;
    return ss.str();
}
