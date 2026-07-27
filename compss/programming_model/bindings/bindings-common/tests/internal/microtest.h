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
#ifndef TESTS_INTERNAL_MICROTEST_H
#define TESTS_INTERNAL_MICROTEST_H

// Tiny header-only-ish unit test framework used by the bindings-common test
// suite. Replaces googletest so the build only requires a C++11 compiler and
// pthreads. Provides just enough surface (TEST/TEST_P, EXPECT_*/ASSERT_*) for
// the existing tests; not intended as a general-purpose framework.

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace microtest {

struct TestCase {
    std::string suite;
    std::string name;
    std::string param;          // empty for non-parameterized tests
    std::function<void()> run;  // wraps any per-test setup + body
};

// Global registry. Populated by static initializers from each TEST/TEST_P.
std::vector<TestCase>& registry();

// Records a failure against the currently running test. Set by the driver
// before invoking each test case.
void record_failure(const char* file, int line, const std::string& msg);
bool current_test_failed();
void clear_current_test_failures();
void set_current_test(const TestCase* tc);

// Number of failures recorded so far in the current test. ASSERT_* samples
// this around its own check so it aborts only on its own failure, rather than
// on any failure an earlier EXPECT_* left behind.
int current_test_failure_count();

// Driver entry point used by the test binary's main().
int run_all(int argc, char** argv);

// dump() — converts an arbitrary value to a printable string for failure
// messages. The std::string overload escapes \n/\t/\\/" so wire-format
// mismatches are easy to compare visually.
template <typename T>
std::string dump(const T& v) {
    std::ostringstream oss;
    oss << v;
    return oss.str();
}
std::string dump(const std::string& s);
std::string dump(const char* s);
std::string dump(bool b);

struct Registrar {
    Registrar(const char* suite,
              const char* name,
              const char* param,
              std::function<void()> fn) {
        TestCase tc;
        tc.suite = suite;
        tc.name = name;
        tc.param = param ? param : "";
        tc.run = std::move(fn);
        registry().push_back(std::move(tc));
    }
};

// Marker exception used by ASSERT_* to abort the current test body.
struct AssertionAbort {};

}  // namespace microtest

#define MT_CONCAT_(a, b) a##b
#define MT_CONCAT(a, b) MT_CONCAT_(a, b)
#define MT_UNIQUE(prefix) MT_CONCAT(prefix, __LINE__)

// ---------------------------------------------------------------------------
// Plain TEST(suite, name)
// ---------------------------------------------------------------------------
#define TEST(suite_, name_)                                                   \
    static void suite_##_##name_##_body();                                    \
    static const ::microtest::Registrar MT_UNIQUE(_mt_reg_)(                  \
        #suite_, #name_, "", &suite_##_##name_##_body);                       \
    static void suite_##_##name_##_body()

// ---------------------------------------------------------------------------
// TEST_P(fixture, name)
//
// Each fixture type must provide:
//
//     template <void(*Body)()>
//     static int register_test(const char* name);
//
// which is responsible for pushing one entry per parameter into the registry.
// Test bodies do not take a parameter — per-test setup is performed by a
// helper invoked from the registered runner.
// ---------------------------------------------------------------------------
#define TEST_P(fixture_, name_)                                               \
    static void fixture_##_##name_##_body();                                  \
    static const int fixture_##_##name_##_reg =                               \
        fixture_::template register_test<&fixture_##_##name_##_body>(#name_); \
    static void fixture_##_##name_##_body()

// ---------------------------------------------------------------------------
// EXPECT_* / ASSERT_*
// ---------------------------------------------------------------------------
#define MT_FAIL(msg_)                                                         \
    ::microtest::record_failure(__FILE__, __LINE__, (msg_))

#define MT_BINARY_EXPECT(op_, opname_, a_, b_)                                \
    do {                                                                      \
        auto&& _va = (a_);                                                    \
        auto&& _vb = (b_);                                                    \
        if (!(_va op_ _vb)) {                                                 \
            std::ostringstream _oss;                                          \
            _oss << opname_ "(" #a_ ", " #b_ ") failed:\n"                    \
                 << "  " #a_ " = " << ::microtest::dump(_va) << "\n"          \
                 << "  " #b_ " = " << ::microtest::dump(_vb);                 \
            MT_FAIL(_oss.str());                                              \
        }                                                                     \
    } while (0)

#define MT_BOOL_EXPECT(cond_, expected_, opname_)                             \
    do {                                                                      \
        bool _vc = static_cast<bool>(cond_);                                  \
        if (_vc != (expected_)) {                                             \
            std::ostringstream _oss;                                          \
            _oss << opname_ "(" #cond_ ") failed:\n"                          \
                 << "  expected " << ((expected_) ? "true" : "false")         \
                 << ", got " << (_vc ? "true" : "false");                     \
            MT_FAIL(_oss.str());                                              \
        }                                                                     \
    } while (0)

#define EXPECT_EQ(a_, b_) MT_BINARY_EXPECT(==, "EXPECT_EQ", a_, b_)
#define EXPECT_NE(a_, b_) MT_BINARY_EXPECT(!=, "EXPECT_NE", a_, b_)
#define EXPECT_LT(a_, b_) MT_BINARY_EXPECT(<, "EXPECT_LT", a_, b_)
#define EXPECT_GE(a_, b_) MT_BINARY_EXPECT(>=, "EXPECT_GE", a_, b_)
#define EXPECT_TRUE(c_) MT_BOOL_EXPECT(c_, true, "EXPECT_TRUE")
#define EXPECT_FALSE(c_) MT_BOOL_EXPECT(c_, false, "EXPECT_FALSE")

// Runs one EXPECT_* and aborts the test only if *that* check was the thing
// that failed. Comparing the failure count before and after keeps an earlier
// EXPECT_* failure from turning every later ASSERT_* into an abort.
#define MT_ASSERT(expect_stmt_)                                               \
    do {                                                                      \
        const int _mt_before = ::microtest::current_test_failure_count();      \
        expect_stmt_;                                                         \
        if (::microtest::current_test_failure_count() != _mt_before) {         \
            throw ::microtest::AssertionAbort{};                              \
        }                                                                     \
    } while (0)

#define ASSERT_EQ(a_, b_) MT_ASSERT(EXPECT_EQ(a_, b_))
#define ASSERT_NE(a_, b_) MT_ASSERT(EXPECT_NE(a_, b_))
#define ASSERT_GE(a_, b_) MT_ASSERT(EXPECT_GE(a_, b_))
#define ASSERT_TRUE(c_) MT_ASSERT(EXPECT_TRUE(c_))
#define ASSERT_FALSE(c_) MT_ASSERT(EXPECT_FALSE(c_))

#endif  // TESTS_INTERNAL_MICROTEST_H
