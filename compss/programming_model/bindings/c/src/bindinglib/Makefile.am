#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

include_HEADERS = c_compss.h c_compss_commons.h CBindingCache.h c_compss_base64.h
lib_LTLIBRARIES = libcbindings.la

libcbindings_la_SOURCES = c_compss.cc c_compss_base64.c
libcbindings_la_CPPFLAGS = -I../../../bindings-common/include ${BOOST_CPPFLAGS} $(DEFINE)
libcbindings_la_LDFLAGS = -L../../../bindings-common/lib -lbindings_common

ACLOCAL_AMFLAGS =-I m4

clean:
	rm -f *.o *.lo *~
