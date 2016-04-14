# Copyright 2013 Barcelona Supercomputing Center (www.bsc.es)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin_PROGRAMS = PACKAGE

DEFINE = -DAPPNAME=\"PACKAGE\" \
	-DPRJ_FILE=\"$(PRJ_FILE)\" \
	-DRES_FILE=\"$(RES_FILE)\" \
	-DMASTER_DIR=\"$(MASTER_DIR)\" \
	-DIT_HOME=\"$(IT_HOME)\" \
	-DGAT_LOCATION=\"$(GAT_LOCATION)\" \
	-DPROACTIVE_HOME=\"$(PROACTIVE_HOME)\"

PACKAGE_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(GS_HOME)/../bindings-common/include -I$(GS_HOME)/include -w

PACKAGE_SOURCES = empty.cc \
	PACKAGE-stubs.cc \
	PACKAGE.cc

PACKAGE_LDFLAGS = 

PACKAGE_LDADD = -L../../lib -lmaster -L$(JAVA_HOME)/jre/lib/$(ARCH)/$(SUBFLDR) -ljvm -ldl -L$(GS_HOME)/../bindings-common/lib -lbindings_common -L$(GS_HOME)/lib -lcbindings -lboost_iostreams -lboost_serialization

GSSTUBGEN_FLAGS =

PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(GSSTUBGEN) $(GSSTUBGEN_FLAGS) PACKAGE.idl


