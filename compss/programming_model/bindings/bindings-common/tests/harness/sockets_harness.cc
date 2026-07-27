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
#include "transport.h"
#include "runtime_api.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

#include "GS_compss.h"
#include "common_test_types.h"

// Test-only hook defined in GS_compss.cc; resets the cached workflow so each
// test sees a fresh registration against the current runtime.
extern "C" void GS_test_reset_workflow(void);

struct SocketsHarness : public TransportHarness {
    int listenFd{-1};
    int harnessFd{-1};
    std::string socketPath;
    mutable std::string commandLog;

    ~SocketsHarness() override {
        closeAll();
        cleanupSocketPath();
    }

    void reset() override {
        closeAll();
        cleanupSocketPath();

        socketPath = makeSocketPath();
        if (socketPath.empty()) {
            return;
        }

        listenFd = createListenSocket(socketPath);
        if (listenFd < 0) {
            cleanupSocketPath();
            return;
        }

        GS_set_socket_endpoint(const_cast<char*>(socketPath.c_str()));
        GS_test_reset_workflow();

        harnessFd = acceptConnection(listenFd);
        closeFd(listenFd);

        if (harnessFd < 0) {
            cleanupSocketPath();
            return;
        }

        setNonBlocking(harnessFd);
        commandLog.clear();
        unlinkAfterAccept();
    }

    void enqueueResponse(const std::string &line) override {
        sendAll(harnessFd, line);
    }

    std::string commands() const override {
        drainCommandSocket();
        return commandLog;
    }

private:
    void unlinkAfterAccept() {
        if (!socketPath.empty()) {
            ::unlink(socketPath.c_str());
        }
    }

    static void closeFd(int& fd) {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    void closeAll() {
        closeFd(listenFd);
        closeFd(harnessFd);
    }

    static void setNonBlocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1) {
            return;
        }
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    static void sendAll(int fd, const std::string& data) {
        if (fd < 0) {
            return;
        }
        const char* ptr = data.data();
        size_t remaining = data.size();
        while (remaining > 0) {
            ssize_t sent = send(fd, ptr, remaining, 0);
            if (sent < 0) {
                if (errno == EINTR) {
                    continue;
                }
                break;
            }
            ptr += sent;
            remaining -= static_cast<size_t>(sent);
        }
    }

    void drainCommandSocket() const {
        if (harnessFd < 0) {
            return;
        }
        char buffer[1024];
        while (true) {
            ssize_t received = recv(harnessFd, buffer, sizeof(buffer), 0);
            if (received > 0) {
                commandLog.append(buffer, static_cast<size_t>(received));
                continue;
            }
            if (received == 0) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            return;
        }
    }

    static std::string makeSocketPath() {
        char tmpl[] = "/tmp/compss_socket_XXXXXX";
        int fd = mkstemp(tmpl);
        if (fd == -1) {
            return std::string();
        }
        ::close(fd);
        ::unlink(tmpl);
        return std::string(tmpl);
    }

    static int createListenSocket(const std::string& path) {
        int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }

        sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        if (path.size() >= sizeof(addr.sun_path)) {
            ::close(fd);
            return -1;
        }

        ::memcpy(addr.sun_path, path.c_str(), path.size() + 1);

        if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            return -1;
        }

        if (listen(fd, 1) != 0) {
            ::close(fd);
            return -1;
        }

        return fd;
    }

    int acceptConnection(int fd) const {
        if (fd < 0) {
            return -1;
        }
        int accepted = ::accept(fd, nullptr, nullptr);
        return accepted;
    }

    void cleanupSocketPath() {
        if (!socketPath.empty()) {
            ::unlink(socketPath.c_str());
            socketPath.clear();
        }
    }
};

struct SocketsRuntime : public RuntimeApi {
    void setEndpoints(const std::string &commandPath, const std::string &resultPath) override {
        (void)resultPath;
        GS_set_socket_endpoint(const_cast<char*>(commandPath.c_str()));
    }
};

// Factory functions for tests
extern "C" TransportHarness* makeSocketsHarness() { return new SocketsHarness(); }
extern "C" RuntimeApi* makeSocketsRuntime() { return new SocketsRuntime(); }
