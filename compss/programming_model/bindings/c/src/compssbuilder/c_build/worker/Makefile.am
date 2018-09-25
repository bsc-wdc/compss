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

bin_PROGRAMS = nio_worker_c worker_c

ACLOCAL_AMFLAGS=-I m4

worker_c_CPPFLAGS =  -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

worker_c_SOURCES = worker_c.cc \
        PACKAGE-executor.cc \
        PACKAGE-functions.cc
     
worker_c_LDFLAGS =

worker_c_LDADD = -L../../lib -lworker -ldl -L$(LIBJAVAFOLDER) -ljvm -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lboost_system  

nio_worker_c_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

nio_worker_c_SOURCES = nio_worker_c.cc \
        PACKAGE-executor.cc \
        PACKAGE-functions.cc
        
nio_worker_c_LDFLAGS = 

nio_worker_c_LDADD = -L../../lib -lworker -ldl -L$(LIBJAVAFOLDER) -ljvm -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lboost_system -lpthread 

COMPSSGEN_FLAGS = 

PACKAGE-executor.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(COMPSSGEN) $(COMPSSGEN_FLAGS) PACKAGE.idl
	
