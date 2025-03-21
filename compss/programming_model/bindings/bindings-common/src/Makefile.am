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

include_HEADERS = BindingDataManager.h BindingExecutor.h JavaNioConnStreamBuffer.h AbstractExecutor.h AbstractCache.h compss_worker.h common.h GS_compss.h param_metadata.h data_type.h compss_jni.h compss_pipes.h common_jni.h
lib_LTLIBRARIES = libbindings_common.la

libbindings_common_la_SOURCES = BindingDataManager.cc BindingExecutor.cc JavaNioConnStreamBuffer.cc AbstractCache.cc compss_worker.cc common.cc GS_compss.cc compss_jni.cc compss_pipes.cc common_jni.cc
libbindings_common_la_CPPFLAGS = -I$(JAVA_HOME)/include $(CPP_OS) -I.
libbindings_common_la_LDFLAGS = -shared -L$(JAVA_LIB_DIR) -ljvm

ACLOCAL_AMFLAGS =-I m4

clean:
	rm -f *.o *.lo *~
