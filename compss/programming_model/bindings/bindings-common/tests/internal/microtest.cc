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

#include <cstdio>

namespace microtest {

namespace {
struct DriverState {
    const TestCase* current = nullptr;
    int failures_in_current = 0;
};

DriverState& driver() {
    static DriverState s;
    return s;
}
}  // namespace

std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

void set_current_test(const TestCase* tc) {
    driver().current = tc;
    driver().failures_in_current = 0;
}

void clear_current_test_failures() {
    driver().failures_in_current = 0;
}

bool current_test_failed() {
    return driver().failures_in_current > 0;
}

int current_test_failure_count() {
    return driver().failures_in_current;
}

void record_failure(const char* file, int line, const std::string& msg) {
    DriverState& d = driver();
    ++d.failures_in_current;
    std::fprintf(stderr, "%s:%d: Failure\n%s\n", file, line, msg.c_str());
}

std::string dump(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    out += '"';
    for (char c : s) {
        switch (c) {
            case '\n': out += "\\n"; break;
            case '\t': out += "\\t"; break;
            case '\r': out += "\\r"; break;
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            default:   out += c; break;
        }
    }
    out += '"';
    return out;
}

std::string dump(const char* s) {
    if (s == nullptr) {
        return "(null)";
    }
    return dump(std::string(s));
}

std::string dump(bool b) {
    return b ? "true" : "false";
}

}  // namespace microtest
