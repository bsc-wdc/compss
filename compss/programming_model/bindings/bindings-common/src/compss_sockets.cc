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

#include "compss_sockets.h"
#include "command_builders.h"
#include "compss_interface.h"

#include <errno.h>
#include <atomic>
#include <chrono>
#include <limits>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>

namespace {
typedef void (*SocketRetrySleepHook)(long seconds);

std::atomic<int> socket_fd{-1};
std::string inbound_buffer;
std::atomic<uint64_t> connector_generation{0};
std::atomic<SocketRetrySleepHook> retry_sleep_hook{nullptr};

void reset_socket_state() {
    int fd = socket_fd.exchange(-1);
    if (fd >= 0) {
        close(fd);
    }
    inbound_buffer.clear();
}

bool try_parse_fd(const char* endpoint, int& fd_out) {
    if (endpoint == nullptr || *endpoint == '\0') {
        return false;
    }

    char* endptr = nullptr;
    errno = 0;
    long candidate = strtol(endpoint, &endptr, 10);
    if (endptr == endpoint || *endptr != '\0' || errno == ERANGE) {
        return false;
    }
    if (candidate < 0 || candidate > std::numeric_limits<int>::max()) {
        return false;
    }
    fd_out = static_cast<int>(candidate);
    return true;
}

int connect_unix_socket(const char* path) {
    if (path == nullptr || *path == '\0') {
        return -1;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Failed to create socket: %s\n", strerror(errno));
        return -1;
    }

    sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    size_t path_len = strlen(path);
    if (path_len >= sizeof(addr.sun_path)) {
        debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Socket path too long: %s\n", path);
        close(fd);
        return -1;
    }

    memcpy(addr.sun_path, path, path_len + 1);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Failed to connect to %s: %s\n", path, strerror(errno));
        close(fd);
        return -1;
    }

    return fd;
}

void retry_sleep_for(long seconds) {
    SocketRetrySleepHook hook = retry_sleep_hook.load(std::memory_order_acquire);
    if (hook != nullptr) {
        hook(seconds);
        return;
    }
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
}

void start_background_connector(const std::string& endpoint, uint64_t generation) {
    std::thread(
        [endpoint, generation]() {
            debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Starting background retries for %s\n", endpoint.c_str());
            while (connector_generation.load() == generation) {
                int fd = connect_unix_socket(endpoint.c_str());
                if (connector_generation.load() != generation) {
                    if (fd >= 0) {
                        close(fd);
                    }
                    break;
                }
                if (fd >= 0) {
                    int previous = socket_fd.exchange(fd);
                    if (previous >= 0 && previous != fd) {
                        close(previous);
                    }
                    debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Connected to UNIX socket %s (fd=%d) after retry\n",
                                 endpoint.c_str(),
                                 fd);
                    break;
                }
                debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Retry connection to %s in 60 seconds\n", endpoint.c_str());
                for (int i = 0; i < 60 && connector_generation.load() == generation; ++i) {
                    retry_sleep_for(1);
                }
            }
        })
        .detach();
}

void socket_command_writer(const char* data, size_t length) {
    int fd = socket_fd.load();
    if (fd < 0 || data == nullptr || length == 0) {
        return;
    }

    const char* cursor = data;
    size_t remaining = length;
    while (remaining > 0) {
        ssize_t written = send(fd, cursor, remaining, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        cursor += written;
        remaining -= static_cast<size_t>(written);
    }
}

char* socket_command_reader() {
    int fd = socket_fd.load();
    if (fd < 0) {
        return strdup("");
    }

    while (true) {
        size_t newline_pos = inbound_buffer.find('\n');
        if (newline_pos != std::string::npos) {
            std::string line = inbound_buffer.substr(0, newline_pos);
            inbound_buffer.erase(0, newline_pos + 1);
            return strdup(line.c_str());
        }

        char chunk[1024];
        ssize_t read_bytes = recv(fd, chunk, sizeof(chunk), 0);
        if (read_bytes < 0) {
            if (errno == EINTR) {
                continue;
            }
            return strdup("");
        }
        if (read_bytes == 0) {
            std::string line = inbound_buffer;
            inbound_buffer.clear();
            return strdup(line.c_str());
        }
        inbound_buffer.append(chunk, static_cast<size_t>(read_bytes));
    }
}

void socket_send_command(const std::string& command) {
    socket_command_writer(command.c_str(), command.size());
}

std::string socket_read_line() {
    char* raw = socket_command_reader();
    if (raw == nullptr) {
        return std::string();
    }
    std::string line(raw);
    free(raw);
    return line;
}

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
} // anonymous namespace

// Runtime control ----------------------------------------------------------------

void SOCKET_On(void) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_On\n");
    init_env_vars();
    debug_printf("[BINDING-COMMONS] - @SOCKET_On NOT CURRENTLY IMPLEMENTED FOR SOCKETS\n");
}

void SOCKET_set_endpoint(char* endpoint) {
    init_env_vars();

    uint64_t generation = connector_generation.fetch_add(1) + 1;

    if (endpoint == nullptr) {
        reset_socket_state();
        return;
    }

    reset_socket_state();

    int parsed_fd = -1;
    if (try_parse_fd(endpoint, parsed_fd)) {
        socket_fd.store(parsed_fd);
        inbound_buffer.clear();
        debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Using provided file descriptor %d\n", parsed_fd);
        return;
    }

    std::string endpoint_path(endpoint);
    int fd = connect_unix_socket(endpoint_path.c_str());
    if (fd < 0) {
        debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Unable to establish socket connection. Scheduling retries every 60 seconds.\n");
        start_background_connector(endpoint_path, generation);
        return;
    }

    socket_fd.store(fd);
    inbound_buffer.clear();
    debug_printf("[BINDING-COMMONS] - @SOCKET_set_endpoint - Connected to UNIX socket %s (fd=%d)\n",
                 endpoint_path.c_str(),
                 fd);
}

void SOCKET_set_retry_sleep_hook(SocketRetrySleepHook hook) {
    retry_sleep_hook.store(hook, std::memory_order_release);
}

void SOCKET_clear_retry_sleep_hook(void) {
    retry_sleep_hook.store(nullptr, std::memory_order_release);
}

void SOCKET_read_command(char** command) {
    if (command == nullptr) {
        return;
    }
    char* raw = socket_command_reader();
    if (raw == nullptr) {
        *command = strdup("");
        return;
    }
    *command = raw;
}

void SOCKET_Off(int code) {
    (void)code;
    debug_printf("[BINDING-COMMONS] - @SOCKET_Off\n");
    debug_printf("[BINDING-COMMONS] - @SOCKET_Off NOT CURRENTLY IMPLEMENTED FOR SOCKETS\n");
    debug_printf("[BINDING-COMMONS] - @SOCKET_Off - End\n");
}

void SOCKET_Cancel_Application_Tasks(long appId) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Cancel_Application_Tasks\n");
    socket_send_command(build_cancel_application_tasks_command(appId));
    debug_printf("[BINDING-COMMONS] - @SOCKET_Cancel_Application_Tasks - Tasks cancelled\n");
}

// Task registration / execution ---------------------------------------------------

void SOCKET_ExecuteHttpTask(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce,
    int reduceChunkSize, int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @SOCKET_ExecuteHttpTask - HTTP task execution in bindings-common. \n");
    debug_printf ("[BINDING-COMMONS] NOT YET IMPLEMENTED")
}


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
                       char** implTypeArgs) {
    socket_send_command(build_register_ce_command(ceSignature,
                                                  implSignature,
                                                  implConstraints,
                                                  implType,
                                                  implLocal,
                                                  implIO,
                                                  prolog,
                                                  epilog,
                                                  container,
                                                  numParams,
                                                  implTypeArgs));
}

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
                        void** params) {
    (void)appId;
    debug_printf("[BINDING-COMMONS] - @SOCKET_ExecuteTask - Processing task execution in bindings-common.\n");
    socket_send_command(build_execute_task_class_command(className,
                                                         onFailure,
                                                         timeout,
                                                         methodName,
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
    debug_printf("[BINDING-COMMONS] - @SOCKET_ExecuteTask - Task processed.\n");
}

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
                           void** params) {
    (void)appId;
    debug_printf("[BINDING-COMMONS] - @SOCKET_ExecuteTaskNew - Processing task execution in bindings-common. \n");
    socket_send_command(build_execute_task_signature_command(signature,
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
    debug_printf("[BINDING-COMMONS] - @SOCKET_ExecuteTaskNew - Task processed.\n");
}

// File operations ----------------------------------------------------------------

int SOCKET_Accessed_File(long appId, char* fileName) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Accessed_File - Calling runtime isFileAccessed method  for %s  ...\n",
                 fileName);
    socket_send_command(build_file_accessed_command(appId, fileName));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    int ret = atoi(response);
    debug_printf("[BINDING-COMMONS] - @SOCKET_Accessed_File - Access to file %s marked as %d\n", fileName, ret);
    return ret;
}

void SOCKET_Open_File(long appId, char* fileName, int mode, char** buf) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Open_File - Calling runtime OpenFile method  for %s and mode %d ...\n",
                 fileName,
                 mode);
    socket_send_command(build_open_file_command(appId, fileName, mode));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    *buf = strdup(response ? response : "");
    debug_printf("[BINDING-COMMONS] - @SOCKET_Open_File - COMPSs filename: %s\n", *buf);
}

void SOCKET_Close_File(long appId, char* fileName, int mode) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Close_File - Calling runtime closeFile method...\n");
    socket_send_command(build_close_file_command(appId, fileName, mode));
    debug_printf("[BINDING-COMMONS] - @SOCKET_Close_File - COMPSs filename: %s\n", fileName);
}

void SOCKET_Delete_File(long appId, char* fileName, int waitForData, int applicationDelete) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Delete_File - Calling runtime deleteFile method...\n");
    socket_send_command(build_delete_file_command(appId, fileName, waitForData, applicationDelete));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    int res = atoi(response);
    debug_printf("[BINDING-COMMONS] - @SOCKET_Delete_File - COMPSs filename: %s\n", fileName);
    debug_printf("[BINDING-COMMONS] - @SOCKET_Delete_File - File erased with status: %i\n", (bool)res);
}

void SOCKET_Get_File(long appId, char* fileName) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_File - Calling runtime getFile method...\n");
    socket_send_command(build_get_file_command(appId, fileName));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    int res = atoi(response);
    (void)res;
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_File - COMPSs filename: %s\n", fileName);
}

void SOCKET_Get_Directory(long appId, char* dirName) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_Directory - Calling runtime getDirectory method...\n");
    socket_send_command(build_get_directory_command(appId, dirName));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    int res = atoi(response);
    (void)res;
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_Directory - COMPSs directory: %s\n", dirName);
}

// Synchronisation / task groups ---------------------------------------------------

void SOCKET_Barrier(long appId) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Barrier - Waiting tasks for APP id: %lu\n", appId);
    socket_send_command(build_barrier_command(appId));
    socket_read_line();
    debug_printf("[BINDING-COMMONS] - @SOCKET_Barrier - APP id: %lu\n", appId);
}

void SOCKET_BarrierNew(long appId, int noMoreTasks) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Barrier - Waiting tasks for APP id: %lu\n", appId);
    socket_send_command(build_barrier_new_command(appId, noMoreTasks));
    socket_read_line();
    debug_printf("[BINDING-COMMONS] - @SOCKET_Barrier - APP id: %lu\n", appId);
}

void SOCKET_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_BarrierGroup - COMPSs group name: %s\n", groupName);
    socket_send_command(build_barrier_group_command(appId, groupName));
    bool barrier_finished = false;
    while (!barrier_finished) {
        std::string result = socket_read_line();
        const char* buf = result.c_str();
        if (strncmp(buf, "COMPSS_EXCEPTION", 16) == 0) {
            buf = buf + 22;
            *exceptionMessage = strdup(buf);
            barrier_finished = true;
            debug_printf(
                "[BINDING-COMMONS] - @SOCKET_BarrierGroup - Barrier ended for COMPSs group name: %s with an exception\n",
                groupName);
        } else if (strncmp(buf, "SYNCH", 5) == 0) {
            barrier_finished = true;
            debug_printf("[BINDING-COMMONS] - @SOCKET_BarrierGroup - Barrier ended for COMPSs group name: %s\n",
                         groupName);
        } else {
            debug_printf("[BINDING-COMMONS] - @SOCKET_BarrierGroup - Unexpected command %s to release group: %s\n",
                         buf,
                         groupName);
        }
    }
}

void SOCKET_OpenTaskGroup(char* groupName, int implicitBarrier, long appId) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_OpenTaskGroup - Opening task group %s ...\n", groupName);
    socket_send_command(build_open_task_group_command(appId, groupName, implicitBarrier));
    debug_printf("[BINDING-COMMONS] - @SOCKET_OpenTaskGroup - COMPSs group name: %s\n", groupName);
}

void SOCKET_CloseTaskGroup(char* groupName, long appId) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_CloseTaskGroup - COMPSs group name: %s\n", groupName);
    socket_send_command(build_close_task_group_command(appId, groupName));
    debug_printf("[BINDING-COMMONS] - @SOCKET_CloseTaskGroup - Task group %s closed.\n", groupName);
}

void SOCKET_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_CancelTaskGroup - COMPSs group name: %s\n", groupName);
    socket_send_command(build_cancel_task_group_command(appId, groupName));
    bool barrier_finished = false;
    while (!barrier_finished) {
        std::string result = socket_read_line();
        const char* buf = result.c_str();
        if (strncmp(buf, "COMPSS_EXCEPTION", 16) == 0) {
            buf = buf + 22;
            *exceptionMessage = strdup(buf);
            barrier_finished = true;
            debug_printf(
                "[BINDING-COMMONS] - @SOCKET_BarrierGroup - Barrier ended for COMPSs group name: %s with an exception\n",
                groupName);
        } else if (strncmp(buf, "SYNCH", 5) == 0) {
            barrier_finished = true;
            debug_printf("[BINDING-COMMONS] - @SOCKET_BarrierGroup - Barrier ended for COMPSs group name: %s\n",
                         groupName);
        } else {
            debug_printf("[BINDING-COMMONS] - @SOCKET_BarrierGroup - Unexpected command %s to release group: %s\n",
                         buf,
                         groupName);
        }
    }
    debug_printf("[BINDING-COMMONS] - @SOCKET_ClancelTaskGroup - Task group %s closed.\n", groupName);
}

void SOCKET_Snapshot(long appId) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Snapshot - Snapshot for APP id: %lu\n", appId);
    socket_send_command(build_snapshot_command(appId));
    socket_read_line();
    debug_printf("[BINDING-COMMONS] - @SOCKET_Snapshot - APP id: %lu\n", appId);
}

// Miscellaneous -------------------------------------------------------------------

void SOCKET_Get_AppDir(char** buf) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_AppDir - Getting application directory.\n");
    socket_send_command(build_get_app_dir_command());
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    *buf = strdup(response ? response : "");
}

void SOCKET_Get_MasterWorkingDir(char** buf) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_MasterWorkingDir - Getting master working directory (tmp).\n");
    socket_send_command(build_get_master_working_dir_command());
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    *buf = strdup(response ? response : "");
}

void SOCKET_EmitEvent(int type, long id) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_EmitEvent - Emit Event\n");
    socket_send_command(build_emit_event_command(type, id));
    debug_printf("[BINDING-COMMONS] - @SOCKET_EmitEvent - Event emitted\n");
}

void SOCKET_Get_Object(long appId, char* objectId, char** buf) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_Object - Calling runtime getObject method...\n");
    socket_send_command(build_get_object_command(appId, objectId));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    *buf = strdup(response ? response : "");
    debug_printf("[BINDING-COMMONS] - @SOCKET_Get_Object - COMPSs data id: %s\n", *buf);
}

void SOCKET_Delete_Object(long appId, char* objectId, int** buf) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_Delete_Object - Calling runtime deleteObject method...\n");
    socket_send_command(build_delete_object_command(appId, objectId));
    std::string result = socket_read_line();
    const char* response = response_payload(result);
    int res = atoi(response);
    int* heap_value = static_cast<int*>(malloc(sizeof(int)));
    if (heap_value == NULL) {
        print_error("[BINDING-COMMONS] - @SOCKET_Delete_Object - Allocation failure\n");
        *buf = NULL;
    } else {
        *heap_value = res;
        *buf = heap_value;
    }
    debug_printf("[BINDING-COMMONS] - @SOCKET_Delete_Binding_Object - COMPSs obj: %s\n", objectId);
}

void SOCKET_set_wall_clock(long appId, long wcl, int stopRT) {
    debug_printf("[BINDING-COMMONS] - @SOCKET_set_wall_clock - Setting wall clock for APP id: %ld\n", appId);
    debug_printf("[BINDING-COMMONS] - @SOCKET_set_wall_clock NOT CURRENTLY IMPLEMENTED FOR SOCKETS\n");
}

CompssInterface setup_SOCKET_runtime(char* endpoint){
    SOCKET_set_endpoint(endpoint);

    CompssInterface iface{};
    iface.On = SOCKET_On;
    iface.Off = SOCKET_Off;
    iface.read_command = SOCKET_read_command;
    iface.RegisterCE = SOCKET_RegisterCE;
    iface.ExecuteTask = SOCKET_ExecuteTask;
    iface.ExecuteTaskNew = SOCKET_ExecuteTaskNew;
    iface.ExecuteHttpTask = SOCKET_ExecuteHttpTask;
    iface.Cancel_Application_Tasks = SOCKET_Cancel_Application_Tasks;
    iface.Accessed_File = SOCKET_Accessed_File;
    iface.Open_File = SOCKET_Open_File;
    iface.Close_File = SOCKET_Close_File;
    iface.Delete_File = SOCKET_Delete_File;
    iface.Get_File = SOCKET_Get_File;
    iface.Get_Directory = SOCKET_Get_Directory;
    iface.Barrier = SOCKET_Barrier;
    iface.BarrierNew = SOCKET_BarrierNew;
    iface.BarrierGroup = SOCKET_BarrierGroup;
    iface.OpenTaskGroup = SOCKET_OpenTaskGroup;
    iface.CloseTaskGroup = SOCKET_CloseTaskGroup;
    iface.CancelTaskGroup = SOCKET_CancelTaskGroup;
    iface.Snapshot = SOCKET_Snapshot;
    iface.Get_AppDir = SOCKET_Get_AppDir;
    iface.Get_MasterWorkingDir = SOCKET_Get_MasterWorkingDir;
    iface.EmitEvent = SOCKET_EmitEvent;
    iface.Get_Object = SOCKET_Get_Object;
    iface.Delete_Object = SOCKET_Delete_Object;
    iface.Set_wall_clock = SOCKET_set_wall_clock;
    return iface;
}
