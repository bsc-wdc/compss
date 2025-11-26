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
#ifndef COMMAND_BUILDERS_H
#define COMMAND_BUILDERS_H

#include <string>

// Build the cancel application tasks payload for transport layers.
std::string build_cancel_application_tasks_command(long appId);
std::string build_get_app_dir_command();
std::string build_get_master_working_dir_command();
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
                                             void** params);
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
                                                  void** params);
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
                                      char** implTypeArgs);
std::string build_file_accessed_command(long appId, const char* fileName);
std::string build_open_file_command(long appId, const char* fileName, int mode);
std::string build_close_file_command(long appId, const char* fileName, int mode);
std::string build_delete_file_command(long appId, const char* fileName, int wait, int applicationDelete);
std::string build_get_file_command(long appId, const char* fileName);
std::string build_get_directory_command(long appId, const char* dirName);
std::string build_get_object_command(long appId, const char* objectId);
std::string build_delete_object_command(long appId, const char* objectId);
std::string build_barrier_command(long appId);
std::string build_barrier_new_command(long appId, int noMoreTasks);
std::string build_barrier_group_command(long appId, const char* groupName);
std::string build_open_task_group_command(long appId, const char* groupName, int implicitBarrier);
std::string build_close_task_group_command(long appId, const char* groupName);
std::string build_cancel_task_group_command(long appId, const char* groupName);
std::string build_snapshot_command(long appId);
std::string build_emit_event_command(int type, long id);
std::string build_get_number_of_resources_command(long appId);
std::string build_request_resources_command(long appId, int numResources, const char* groupName);
std::string build_free_resources_command(long appId, int numResources, const char* groupName);

#endif  // COMMAND_BUILDERS_H
