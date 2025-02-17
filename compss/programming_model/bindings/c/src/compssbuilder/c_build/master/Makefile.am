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

bin_PROGRAMS = PACKAGE

ACLOCAL_AMFLAGS=-I m4

PACKAGE_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

PACKAGE_SOURCES = PACKAGE-stubs.cc \
        PACKAGE.cc

PACKAGE_LDFLAGS =

PACKAGE_LDADD = -L../../lib -L$(LIBJAVAFOLDER) -ljvm -ldl -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lpthread -lboost_system

COMPSSGEN_FLAGS =

PACKAGE-executor.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(COMPSSGEN) $(COMPSSGEN_FLAGS) PACKAGE.idl
	
