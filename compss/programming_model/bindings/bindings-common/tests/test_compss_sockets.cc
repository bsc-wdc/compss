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
#include "internal/microtest.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <poll.h>
#include <string>
#include <thread>

#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "internal/compss_sockets_test_api.h"

namespace {

// Create an ephemeral filesystem path for the test UNIX socket.
std::string make_socket_path() {
    char tmpl[] = "/tmp/compss_socket_test_XXXXXX";
    int fd = ::mkstemp(tmpl);
    if (fd >= 0) {
        ::close(fd);
        ::unlink(tmpl);
        return std::string(tmpl);
    }
    return std::string();
}

// Flip the socket into non-blocking mode so reads do not stall the test.
void set_non_blocking(int fd) {
    if (fd < 0) {
        return;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return;
    }
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Minimal UNIX domain server that accepts a single connection and records payloads.
class UnixSocketServer {
public:
    UnixSocketServer()
        : listen_fd_(-1), client_fd_(-1), path_(make_socket_path()) {}

    ~UnixSocketServer() {
        close_client();
        close_listen();
        cleanup_path();
    }

    const std::string& path() const { return path_; }

    bool listen() {
        if (path_.empty()) {
            return false;
        }
        if (listen_fd_ >= 0) {
            return true;
        }

        cleanup_path();

        listen_fd_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return false;
        }

        sockaddr_un addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        if (path_.size() >= sizeof(addr.sun_path)) {
            close_listen();
            return false;
        }

        std::memcpy(addr.sun_path, path_.c_str(), path_.size() + 1);

        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            close_listen();
            return false;
        }

        if (::listen(listen_fd_, 1) != 0) {
            close_listen();
            return false;
        }

        return true;
    }

    int accept_blocking(std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        if (listen_fd_ < 0) {
            return -1;
        }

        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (true) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                return -1;
            }
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            int remaining_ms = static_cast<int>(remaining.count());

            struct pollfd pfd;
            pfd.fd = listen_fd_;
            pfd.events = POLLIN;
            pfd.revents = 0;

            int rc = ::poll(&pfd, 1, remaining_ms);
            if (rc < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return -1;
            }
            if (rc == 0) {
                return -1;
            }

            int fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd >= 0) {
                set_non_blocking(fd);
                close_client();
                client_fd_ = fd;
                return fd;
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return -1;
        }
    }

    std::string drain_commands(std::chrono::milliseconds timeout = std::chrono::milliseconds(200)) {
        std::string data;
        if (client_fd_ < 0) {
            return data;
        }

        auto deadline = std::chrono::steady_clock::now() + timeout;
        char buffer[256];

        while (true) {
            ssize_t received = ::recv(client_fd_, buffer, sizeof(buffer), 0);
            if (received > 0) {
                data.append(buffer, static_cast<size_t>(received));
                continue;
            }
            if (received == 0) {
                close_client();
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                if (std::chrono::steady_clock::now() >= deadline) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            break;
        }

        return data;
    }

private:
    void close_client() {
        if (client_fd_ >= 0) {
            ::close(client_fd_);
            client_fd_ = -1;
        }
    }

    void close_listen() {
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    void cleanup_path() {
        if (!path_.empty()) {
            ::unlink(path_.c_str());
        }
    }

    int listen_fd_;
    int client_fd_;
    std::string path_;
};

// Retry hook that skips real sleeping; used to fast-forward reconnection loops.
void no_delay_sleep(long) {}

// Ensures the retry hook is restored after each test.
struct ScopedRetryHook {
    explicit ScopedRetryHook(SocketRetrySleepHook hook) : active_(true) {
        SOCKET_set_retry_sleep_hook(hook);
    }

    ~ScopedRetryHook() {
        if (active_) {
            SOCKET_clear_retry_sleep_hook();
        }
    }

    ScopedRetryHook(const ScopedRetryHook&) = delete;
    ScopedRetryHook& operator=(const ScopedRetryHook&) = delete;

private:
    bool active_;
};

} // namespace

// When the server is ready immediately, the client should connect and deliver commands once.
TEST(CompssSocketsTest, ConnectsWhenServerReady) {
    // Clean slate before setting up a new connection.
    SOCKET_set_endpoint(nullptr);

    // Prepare the listener before initiating the endpoint so the client connects on first attempt.
    UnixSocketServer server;
    ASSERT_FALSE(server.path().empty());
    ASSERT_TRUE(server.listen());

    std::atomic<int> accepted_fd{-1};
    std::thread accept_thread([&]() {
        // Block until the incoming connection arrives (with timeout guarding against hangs).
        accepted_fd.store(server.accept_blocking());
    });

    std::thread connector_thread([&]() {
        // Invoke the production entry point; this should connect on the first attempt.
        SOCKET_set_endpoint(const_cast<char*>(server.path().c_str()));
    });

    // Wait for both threads so the connection is established before asserting.
    connector_thread.join();
    accept_thread.join();
    ASSERT_GE(accepted_fd.load(), 0);

    // Issue a command to confirm the transport path is functional.
    SOCKET_WF_cancelApplicationTasks(nullptr);

    std::string commands = server.drain_commands();
    EXPECT_NE(commands.find("CANCEL_APPLICATION_TASKS"), std::string::npos);

    SOCKET_set_endpoint(nullptr);
}

// When the server comes up later, the retry thread should reconnect without real delays.
TEST(CompssSocketsTest, RetriesUntilServerAvailable) {
    // Reset any previous socket state.
    SOCKET_set_endpoint(nullptr);

    // No listener yet: the first connect attempt must fail and trigger the retry loop.
    UnixSocketServer server;
    ASSERT_FALSE(server.path().empty());

    // Replace the retry sleep with a no-op to make the loop effectively instantaneous.
    ScopedRetryHook hook(&no_delay_sleep);

    SOCKET_set_endpoint(const_cast<char*>(server.path().c_str()));

    ASSERT_TRUE(server.listen());

    std::atomic<int> accepted_fd{-1};
    std::thread accept_thread([&]() {
        // The retry loop should eventually land here once the server is up.
        accepted_fd.store(server.accept_blocking());
    });

    accept_thread.join();
    ASSERT_GE(accepted_fd.load(), 0);

    SOCKET_WF_cancelApplicationTasks(nullptr);

    // Commands should arrive once the late connection succeeds.
    std::string commands = server.drain_commands();
    EXPECT_NE(commands.find("CANCEL_APPLICATION_TASKS"), std::string::npos);

    SOCKET_set_endpoint(nullptr);
}
