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
#ifndef TESTS_HARNESS_RUNTIME_API_H
#define TESTS_HARNESS_RUNTIME_API_H

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include "common_test_types.h"
#include "GS_compss.h"
#include "common.h"

// Forward declaration for PipesHarness to allow casting in tests
struct PipesHarness;

struct RuntimeApi {
    virtual ~RuntimeApi() {}

    virtual void setEndpoints(const std::string &commandPath, const std::string &resultPath) = 0;

    virtual void on() { GS_On(); }

    virtual void readCommand(std::string &out) {
        char* buffer = nullptr;
        GS_read_command(&buffer);
        if (buffer != nullptr) {
            out.assign(buffer);
            free(buffer);
        } else {
            out.clear();
        }
    }

    virtual void off(int code) { GS_Off(code); }

    virtual void cancelApplicationTasks(long appId) { GS_Cancel_Application_Tasks(appId); }

    virtual void registerCE(const std::string& ceSignature,
                            const std::string& implSignature,
                            const std::string& implConstraints,
                            const std::string& implType,
                            const std::string& implLocal,
                            const std::string& implIO,
                            const std::vector<std::string>& prolog,
                            const std::vector<std::string>& epilog,
                            const std::vector<std::string>& container,
                            int numArgs,
                            const std::vector<std::string>& implTypeArgs) {
        std::vector<char*> c_prolog(prolog.size());
        for (size_t i = 0; i < prolog.size(); ++i) {
            c_prolog[i] = const_cast<char*>(prolog[i].c_str());
        }

        std::vector<char*> c_epilog(epilog.size());
        for (size_t i = 0; i < epilog.size(); ++i) {
            c_epilog[i] = const_cast<char*>(epilog[i].c_str());
        }

        std::vector<char*> c_container(container.size());
        for (size_t i = 0; i < container.size(); ++i) {
            c_container[i] = const_cast<char*>(container[i].c_str());
        }

        std::vector<char*> c_implTypeArgs(implTypeArgs.size());
        for (size_t i = 0; i < implTypeArgs.size(); ++i) {
            c_implTypeArgs[i] = const_cast<char*>(implTypeArgs[i].c_str());
        }

        GS_RegisterCE(const_cast<char*>(ceSignature.c_str()),
                      const_cast<char*>(implSignature.c_str()),
                      const_cast<char*>(implConstraints.c_str()),
                      const_cast<char*>(implType.c_str()),
                      const_cast<char*>(implLocal.c_str()),
                      const_cast<char*>(implIO.c_str()),
                      c_prolog.empty() ? nullptr : c_prolog.data(),
                      c_epilog.empty() ? nullptr : c_epilog.data(),
                      c_container.empty() ? nullptr : c_container.data(),
                      numArgs,
                      c_implTypeArgs.empty() ? nullptr : c_implTypeArgs.data());
    }

    virtual void executeTaskNew(long appId,
                                  const std::string& signature,
                                  const std::string& onFailure,
                                  int timeout,
                                  int priority,
                                  int numNodes,
                                  bool reduce,
                                  int reduceChunkSize,
                                  bool replicated,
                                  bool distributed,
                                  bool hasTarget,
                                  int numReturns,
                                  const std::vector<Parameter>& params) {
        std::vector<void*> allocations;
        std::vector<void*> param_block = prepareParams(params, allocations);
        GS_ExecuteTaskNew(appId,
                          const_cast<char*>(signature.c_str()),
                          const_cast<char*>(onFailure.c_str()),
                          timeout,
                          priority,
                          numNodes,
                          reduce ? 1 : 0,
                          reduceChunkSize,
                          replicated ? 1 : 0,
                          distributed ? 1 : 0,
                          hasTarget ? 1 : 0,
                          numReturns,
                          static_cast<int>(params.size()),
                          param_block.empty() ? nullptr : param_block.data());
        releaseAllocations(allocations);
    }

    virtual int accessedFile(long appId, const std::string& fileName) {
        return GS_Accessed_File(appId, const_cast<char*>(fileName.c_str()));
    }

    virtual void closeFile(long appId, const std::string& fileName, int mode) {
        GS_Close_File(appId, const_cast<char*>(fileName.c_str()), mode);
    }

    virtual int deleteFile(long appId, const std::string& fileName, bool waitForData, bool applicationDelete) {
        GS_Delete_File(appId,
                       const_cast<char*>(fileName.c_str()),
                       waitForData ? 1 : 0,
                       applicationDelete ? 1 : 0);
        return 0;
    }

    virtual int getFile(long appId, const std::string& fileName) {
        GS_Get_File(appId, const_cast<char*>(fileName.c_str()));
        return 0;
    }

    virtual int getDirectory(long appId, const std::string& dirName) {
        GS_Get_Directory(appId, const_cast<char*>(dirName.c_str()));
        return 0;
    }

    virtual void barrier(long appId) { GS_Barrier(appId); }

    virtual void barrierNew(long appId, bool noMoreTasks) { GS_BarrierNew(appId, noMoreTasks ? 1 : 0); }

    virtual std::string barrierGroup(long appId, const std::string& groupName) {
        char* message = nullptr;
        GS_BarrierGroup(appId, const_cast<char*>(groupName.c_str()), &message);
        if (message == nullptr) {
            return {};
        }
        std::string result(message);
        free(message);
        return result;
    }

    virtual void openTaskGroup(const std::string& groupName, bool implicitBarrier, long appId) {
        GS_OpenTaskGroup(const_cast<char*>(groupName.c_str()), implicitBarrier ? 1 : 0, appId);
    }

    virtual void closeTaskGroup(const std::string& groupName, long appId) {
        GS_CloseTaskGroup(const_cast<char*>(groupName.c_str()), appId);
    }

    virtual std::string cancelTaskGroup(const std::string& groupName, long appId) {
        char* message = nullptr;
        GS_CancelTaskGroup(const_cast<char*>(groupName.c_str()), appId, &message);
        if (message == nullptr) {
            return {};
        }
        std::string result(message);
        free(message);
        return result;
    }

    virtual void snapshot(long appId) { GS_Snapshot(appId); }

    virtual std::string getAppDir() {
        char* buffer = nullptr;
        GS_Get_AppDir(&buffer);
        if (buffer == nullptr) {
            return {};
        }
        std::string result(buffer);
        free(buffer);
        return result;
    }

    virtual std::string getMasterWorkingDir() {
        char* buffer = nullptr;
        GS_Get_MasterWorkingDir(&buffer);
        if (buffer == nullptr) {
            return {};
        }
        std::string result(buffer);
        free(buffer);
        return result;
    }

    virtual std::string getObject(long appId, const std::string& objectId) {
        char* buffer = nullptr;
        GS_Get_Object(appId, const_cast<char*>(objectId.c_str()), &buffer);
        if (buffer == nullptr) {
            return {};
        }
        std::string result(buffer);
        free(buffer);
        return result;
    }

    virtual int deleteObject(long appId, const std::string& objectId) {
        int* buffer = nullptr;
        GS_Delete_Object(appId, const_cast<char*>(objectId.c_str()), &buffer);
        (void)buffer;
        return 0;
    }

    virtual void openFile(long appId, const std::string &file, int mode, std::string &out) {
        char* buffer = nullptr;
        GS_Open_File(appId, const_cast<char*>(file.c_str()), mode, &buffer);
        out = buffer ? std::string(buffer) : std::string();
    }

protected:
    static std::vector<void*> prepareParams(const std::vector<Parameter>& params,
                                            std::vector<void*>& allocations) {
        std::vector<void*> block(params.size() * NUM_FIELDS, nullptr);

        for (size_t i = 0; i < params.size(); ++i) {
            const Parameter& p = params[i];
            size_t base = i * NUM_FIELDS;

            auto allocNumeric = [&](auto value) {
                using T = decltype(value);
                T* holder = static_cast<T*>(malloc(sizeof(T)));
                if (holder != nullptr) {
                    *holder = value;
                    block[base + 0] = holder;
                    allocations.push_back(holder);
                }
            };

            auto allocStringLike = [&](const std::string& value) {
                char** holder = static_cast<char**>(malloc(sizeof(char*)));
                if (holder != nullptr) {
                    char* dup = strdup(value.c_str());
                    *holder = dup;
                    block[base + 0] = holder;
                    if (dup != nullptr) {
                        allocations.push_back(dup);
                    }
                    allocations.push_back(holder);
                }
            };

            switch (p.type) {
                case boolean_dt:
                    allocNumeric(static_cast<int>((p.value == "true" || p.value == "1") ? 1 : 0));
                    break;
                case byte_dt:
                    allocNumeric(static_cast<signed char>(std::stoi(p.value)));
                    break;
                case char_dt:
                case wchar_dt:
                    allocNumeric(static_cast<char>(p.value.empty() ? '\0' : p.value[0]));
                    break;
                case short_dt:
                    allocNumeric(static_cast<short>(std::stoi(p.value)));
                    break;
                case int_dt:
                    allocNumeric(std::stoi(p.value));
                    break;
                case long_dt:
                    allocNumeric(std::stol(p.value));
                    break;
                case longlong_dt:
                    allocNumeric(std::stoll(p.value));
                    break;
                case float_dt:
                    allocNumeric(std::stof(p.value));
                    break;
                case double_dt:
                    allocNumeric(std::stod(p.value));
                    break;
                case string_dt:
                case string_64_dt:
                case wstring_dt:
                case file_dt:
                case directory_dt:
                case object_dt:
                case psco_dt:
                case external_psco_dt:
                case binding_object_dt:
                case collection_dt:
                case dict_collection_dt:
                case stream_dt:
                case external_stream_dt:
                case enum_dt:
                case array_char_dt:
                case array_byte_dt:
                case array_short_dt:
                case array_int_dt:
                case array_long_dt:
                case array_float_dt:
                case array_double_dt:
                    allocStringLike(p.value);
                    break;
                case null_dt:
                case void_dt:
                case any_dt:
                default:
                    block[base + 0] = nullptr;
                    break;
            }

            int* type_val = static_cast<int*>(malloc(sizeof(int)));
            if (type_val != nullptr) {
                *type_val = static_cast<int>(p.type);
                block[base + 1] = type_val;
                allocations.push_back(type_val);
            }

            int* dir_val = static_cast<int*>(malloc(sizeof(int)));
            if (dir_val != nullptr) {
                *dir_val = static_cast<int>(p.dir);
                block[base + 2] = dir_val;
                allocations.push_back(dir_val);
            }

            int* io_val = static_cast<int*>(malloc(sizeof(int)));
            if (io_val != nullptr) {
                *io_val = static_cast<int>(p.ioStream);
                block[base + 3] = io_val;
                allocations.push_back(io_val);
            }

            auto allocStringField = [&](size_t index, const std::string& value) {
                char** holder = static_cast<char**>(malloc(sizeof(char*)));
                if (holder != nullptr) {
                    char* dup = strdup(value.c_str());
                    *holder = dup;
                    block[base + index] = holder;
                    if (dup != nullptr) {
                        allocations.push_back(dup);
                    }
                    allocations.push_back(holder);
                }
            };

            allocStringField(4, p.prefix);
            allocStringField(5, p.name);
            allocStringField(6, p.contType);
            allocStringField(7, p.weight);

            int* keep_val = static_cast<int*>(malloc(sizeof(int)));
            if (keep_val != nullptr) {
                *keep_val = p.keepRename ? 1 : 0;
                block[base + 8] = keep_val;
                allocations.push_back(keep_val);
            }
        }

        return block;
    }

    static void releaseAllocations(std::vector<void*>& allocations) {
        for (void* ptr : allocations) {
            free(ptr);
        }
        allocations.clear();
    }
};

#endif // TESTS_HARNESS_RUNTIME_API_H
