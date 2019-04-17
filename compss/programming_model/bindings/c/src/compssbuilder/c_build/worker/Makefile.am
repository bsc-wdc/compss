#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

noinst_LIBRARIES = libfunctions.a
libfunctions_a_SOURCES = PACKAGE-functions.cc

#libfunctions_a_LIBADD     = -L../../lib -ldl -L$(LIBJAVAFOLDER) -ljvm -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lboost_systemi

libfunctions_a_CPPFLAGS =  -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

libfunctions_a_CFLAGS =

bin_PROGRAMS = nio_worker_c worker_c

ACLOCAL_AMFLAGS=-I m4

worker_c_CPPFLAGS =  -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

worker_c_SOURCES = worker_c.cc \
        PACKAGE-executor.cc
     
#worker_c_LDFLAGS   = --Wf,"--board=zedboard,--clock=100,--task_manager,--name=worker_bitstream" #We need to customize the step and the board to use, as a workaround this should work.
worker_c_LDFLAGS    =

worker_c_LDADD = -L../../lib -ldl -L$(LIBJAVAFOLDER) -ljvm -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lboost_system -lpthread libfunctions.a 

nio_worker_c_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -Wall

nio_worker_c_SOURCES = nio_worker_c.cc \
        PACKAGE-executor.cc
        
nio_worker_c_LDFLAGS = 

nio_worker_c_LDADD =  -L../../lib -ldl -L$(LIBJAVAFOLDER) -ljvm -L$(CS_HOME)/../bindings-common/lib/$(TARGET_HOST) -lbindings_common -L$(CS_HOME)/lib/$(TARGET_HOST) -lcbindings -L$(BOOST_LIB) -lboost_iostreams -lboost_serialization -lboost_system -lpthread libfunctions.a

#There are not or's nor and's, so this looks pretty ugly...
if OMPSS2
libfunctions_a_CPPFLAGS  += --ompss-2 -DOMPSS2_ENABLED
libfunctions_a_CFLAGS    += --ompss-2 -DOMPSS2_ENABLED

nio_worker_c_CPPFLAGS    += -DOMPSS2_ENABLED
worker_c_CPPFLAGS        += -DOMPSS2_ENABLED

if CUDA 
libfunctions_a_CPPFLAGS  += --cuda -DCUDA_ENABLED
libfunctions_a_CFLAGS    += --cuda -DCUDA_ENABLED
endif

else    

if OMPSS
libfunctions_a_CPPFLAGS  += --ompss -DOMPSS_ENABLED
libfunctions_a_CFLAGS    += --ompss -DOMPSS_ENABLED

nio_worker_c_CPPFLAGS    += -DOMPSS_ENABLED
worker_c_CPPFLAGS        += -DOMPSS_ENABLED

if CUDA 
libfunctions_a_CPPFLAGS  += --cuda -DCUDA_ENABLED
libfunctions_a_CFLAGS    += --cuda -DCUDA_ENABLED
endif

endif
endif

COMPSSGEN_FLAGS = 

PACKAGE-executor.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(COMPSSGEN) $(COMPSSGEN_FLAGS) PACKAGE.idl
	
