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
#include "transport.h"
#include "runtime_api.h"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>
#include <string.h>

#include "GS_compss.h"
#include "common_test_types.h"

struct TempFile {
    std::string path;
    explicit TempFile(const std::string &prefix) {
        char tmpl[256];
        std::snprintf(tmpl, sizeof(tmpl), "/tmp/%sXXXXXX", prefix.c_str());
        int fd = mkstemp(tmpl);
        if (fd == -1) {
            path = "/tmp/" + prefix + "_fallback";
        } else {
            ::close(fd);
            path = tmpl;
        }
    }
    ~TempFile() { std::remove(path.c_str()); }
};

static void writeText(const std::string &path, const std::string &text) {
    std::ofstream ofs(path.c_str(), std::ios::out | std::ios::app);
    ofs << text;
}

static std::string readAll(const std::string &path) {
    std::ifstream ifs(path.c_str());
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    return buffer.str();
}

struct PipesHarness : public TransportHarness {
    TempFile cmd{ "compss_cmd_" };
    TempFile res{ "compss_res_" };

    void reset() override {
        std::ofstream(cmd.path.c_str(), std::ios::trunc).close();
        std::ofstream(res.path.c_str(), std::ios::trunc).close();
        GS_set_pipes(const_cast<char*>(cmd.path.c_str()), const_cast<char*>(res.path.c_str()));
    }

    void enqueueResponse(const std::string &line) override {
        writeText(res.path, line);
    }

    std::string commands() const override { return readAll(cmd.path); }
};

struct PipesRuntime : public RuntimeApi {
    void setEndpoints(const std::string &commandPath, const std::string &resultPath) override {
        GS_set_pipes(const_cast<char*>(commandPath.c_str()), const_cast<char*>(resultPath.c_str()));
    }
};

// Factory functions for tests
extern "C" TransportHarness* makePipesHarness() { return new PipesHarness(); }
extern "C" RuntimeApi* makePipesRuntime() { return new PipesRuntime(); }

