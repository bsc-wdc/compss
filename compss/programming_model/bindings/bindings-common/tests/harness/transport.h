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
#ifndef TESTS_HARNESS_TRANSPORT_H
#define TESTS_HARNESS_TRANSPORT_H

#include <string>

// Lightweight interface that allows the socket and pipes harnesses used in the
// integration tests to be manipulated polymorphically.
struct TransportHarness {
    virtual ~TransportHarness() = default;

    virtual void reset() = 0;
    virtual void enqueueResponse(const std::string& line) = 0;
    virtual std::string commands() const = 0;
};

#endif // TESTS_HARNESS_TRANSPORT_H
