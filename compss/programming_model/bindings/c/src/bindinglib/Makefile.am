#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
BOOST_INCLUDE ?= /usr/include/boost/

include_HEADERS = generated_executor.h customStream.h c_compss.h c_compss_commons.h CBindingCache.h CBindingExecutor.h c_compss_base64.h
lib_LTLIBRARIES = libcbindings.la

libcbindings_la_SOURCES = CBindingExecutor.cc c_compss.cc c_compss_base64.c
libcbindings_la_CPPFLAGS = -Wall -I../../../bindings-common/include -I$(JAVA_HOME)/include $(CPP_OS) -I${BOOST_ROOT}/include -I${BOOST_INCLUDE} ${BOOST_CPPFLAGS} $(DEFINE)
libcbindings_la_LDFLAGS = -L../../../bindings-common/lib -lbindings_common -lpthread 

ACLOCAL_AMFLAGS =-I m4

clean:
	rm -f *.o *.lo *~
