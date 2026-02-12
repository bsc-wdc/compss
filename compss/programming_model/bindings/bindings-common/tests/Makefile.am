#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

AM_CPPFLAGS = -I$(srcdir)/stubs -I$(srcdir)/harness -I$(top_srcdir)/src

# Run tests with debug prints enabled so both branches of debug_printf are exercised
AM_TESTS_ENVIRONMENT = COMPSS_BINDINGS_DEBUG=1

# Allow overriding GTest flags via environment when configuring, e.g.:
#   CXXFLAGS="..." CPPFLAGS="..." LDFLAGS="..." LIBS="-lgtest -lpthread"
# If not provided, try default system libs.
AM_LDFLAGS =

# Enable coverage on test builds
AM_CXXFLAGS = -O0 -g --coverage
AM_LDFLAGS += --coverage

noinst_LTLIBRARIES = libpipes_only.la libharness.la
libpipes_only_la_SOURCES = $(top_srcdir)/src/compss_pipes.cc \
                           $(top_srcdir)/src/compss_sockets.cc \
                           $(top_srcdir)/src/common.cc \
                           $(top_srcdir)/src/command_builders.cc
libpipes_only_la_CPPFLAGS = -I$(srcdir)/stubs -I$(top_srcdir)/src
libharness_la_SOURCES = harness/transport.h harness/runtime_api.h harness/pipes_harness.cc harness/sockets_harness.cc harness/common_test_types.h

check_PROGRAMS = test_gs_compss
TESTS = $(check_PROGRAMS)

test_gs_compss_SOURCES = test_gs_compss.cc test_compss_sockets.cc
test_gs_compss_LDADD = $(top_builddir)/tests/libharness.la \
                          $(top_builddir)/tests/libpipes_only.la \
                          $(top_builddir)/src/libbindings_common.la \
                          -lgtest -lgtest_main -lpthread

clean:
	rm -f *.o *.lo *~ $(check_PROGRAMS)


# Reserved for future local test hooks
check-local:
