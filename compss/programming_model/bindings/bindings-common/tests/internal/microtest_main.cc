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

// Test driver. Walks the registry populated by static initializers from each
// TEST/TEST_P macro and runs every case, capturing failures and printing
// gtest-compatible status lines so the autotools test driver still recognises
// pass/fail output.

#include "internal/microtest.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::string format_full_name(const microtest::TestCase& tc) {
    std::string out = tc.suite + "." + tc.name;
    if (!tc.param.empty()) {
        out += "/" + tc.param;
    }
    return out;
}

// Glob matcher supporting '*' (any run, including empty) and '?' (one char).
// Iterative with backtracking, so it needs no recursion depth guard.
bool glob_match(const std::string& pat, const std::string& str) {
    std::size_t p = 0, s = 0;
    std::size_t star = std::string::npos, star_s = 0;

    while (s < str.size()) {
        if (p < pat.size() && (pat[p] == '?' || pat[p] == str[s])) {
            ++p;
            ++s;
        } else if (p < pat.size() && pat[p] == '*') {
            star = p++;        // remember the '*' so we can extend its match
            star_s = s;
        } else if (star != std::string::npos) {
            p = star + 1;      // backtrack: let the '*' swallow one more char
            s = ++star_s;
        } else {
            return false;
        }
    }
    while (p < pat.size() && pat[p] == '*') {
        ++p;
    }
    return p == pat.size();
}

void split(const std::string& in, char sep, std::vector<std::string>& out) {
    std::size_t start = 0;
    while (true) {
        const std::size_t hit = in.find(sep, start);
        if (hit == std::string::npos) {
            out.push_back(in.substr(start));
            return;
        }
        out.push_back(in.substr(start, hit - start));
        start = hit + 1;
    }
}

// googletest filter syntax: colon-separated positive patterns, then an
// optional '-' followed by colon-separated negative patterns. An empty
// positive list means "everything". A name is selected when it matches at
// least one positive pattern and no negative one.
struct Filter {
    std::vector<std::string> positive;
    std::vector<std::string> negative;
    bool active = false;

    bool matches(const std::string& full) const {
        if (!active) {
            return true;
        }
        bool included = positive.empty();
        for (std::size_t i = 0; i < positive.size() && !included; ++i) {
            included = glob_match(positive[i], full);
        }
        if (!included) {
            return false;
        }
        for (std::size_t i = 0; i < negative.size(); ++i) {
            if (glob_match(negative[i], full)) {
                return false;
            }
        }
        return true;
    }
};

Filter parse_filter(int argc, char** argv) {
    Filter filter;

    static const char* kPrefixes[] = {"--filter=", "--gtest_filter="};
    std::string spec;
    for (int i = 1; i < argc && !filter.active; ++i) {
        for (std::size_t k = 0; k < sizeof(kPrefixes) / sizeof(kPrefixes[0]); ++k) {
            const std::size_t plen = std::strlen(kPrefixes[k]);
            if (std::strncmp(argv[i], kPrefixes[k], plen) == 0) {
                spec = std::string(argv[i] + plen);
                filter.active = true;
                break;
            }
        }
    }
    if (!filter.active) {
        return filter;
    }

    const std::size_t dash = spec.find('-');
    if (dash == std::string::npos) {
        split(spec, ':', filter.positive);
    } else {
        split(spec.substr(0, dash), ':', filter.positive);
        split(spec.substr(dash + 1), ':', filter.negative);
    }

    // Drop the empty strings a leading/trailing ':' or a bare '-' leaves behind,
    // so "-Foo" reads as "everything except Foo" rather than "match nothing".
    filter.positive.erase(
        std::remove(filter.positive.begin(), filter.positive.end(), std::string()),
        filter.positive.end());
    filter.negative.erase(
        std::remove(filter.negative.begin(), filter.negative.end(), std::string()),
        filter.negative.end());

    return filter;
}

}  // namespace

int main(int argc, char** argv) {
    using clock = std::chrono::steady_clock;
    using namespace microtest;

    const Filter filter = parse_filter(argc, argv);

    auto& reg = registry();
    std::vector<const TestCase*> selected;
    selected.reserve(reg.size());
    for (const auto& tc : reg) {
        if (filter.matches(format_full_name(tc))) {
            selected.push_back(&tc);
        }
    }

    // Running nothing is a failure, never a pass. This catches a filter that
    // matched no test as well as a registry left empty by a static
    // initialization or link problem -- both of which would otherwise report
    // success without having executed a single assertion.
    if (selected.empty()) {
        std::fprintf(stderr,
                     "[  FAILED  ] no tests to run (%zu registered)\n",
                     reg.size());
        std::fflush(stderr);
        return 1;
    }

    std::printf("[==========] Running %zu tests.\n", selected.size());
    std::fflush(stdout);

    int passed = 0;
    std::vector<std::string> failed_names;
    const auto suite_start = clock::now();

    for (const TestCase* tc : selected) {
        const std::string full = format_full_name(*tc);
        std::printf("[ RUN      ] %s\n", full.c_str());
        std::fflush(stdout);

        set_current_test(tc);
        const auto t0 = clock::now();
        try {
            tc->run();
        } catch (const AssertionAbort&) {
            // Test was aborted by an ASSERT_*. Failure already recorded.
        } catch (const std::exception& e) {
            std::ostringstream oss;
            oss << "Uncaught std::exception: " << e.what();
            record_failure(__FILE__, __LINE__, oss.str());
        } catch (...) {
            record_failure(__FILE__, __LINE__,
                           std::string("Uncaught unknown exception"));
        }
        const auto t1 = clock::now();
        const auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        if (current_test_failed()) {
            std::printf("[  FAILED  ] %s (%lld ms)\n", full.c_str(),
                        static_cast<long long>(ms));
            failed_names.push_back(full);
        } else {
            std::printf("[       OK ] %s (%lld ms)\n", full.c_str(),
                        static_cast<long long>(ms));
            ++passed;
        }
        std::fflush(stdout);
    }

    const auto suite_end = clock::now();
    const auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              suite_end - suite_start)
                              .count();

    std::printf("[==========] %zu tests ran. (%lld ms total)\n",
                selected.size(), static_cast<long long>(total_ms));
    std::printf("[  PASSED  ] %d tests.\n", passed);
    if (!failed_names.empty()) {
        std::printf("[  FAILED  ] %zu tests, listed below:\n",
                    failed_names.size());
        for (const auto& name : failed_names) {
            std::printf("[  FAILED  ] %s\n", name.c_str());
        }
    }
    std::fflush(stdout);

    return failed_names.empty() ? 0 : 1;
}
