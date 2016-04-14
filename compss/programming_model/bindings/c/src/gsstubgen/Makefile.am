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

AM_YFLAGS = -d
ACLOCAL_AMFLAGS=-I m4 

bin_PROGRAMS = gsstubgen
sbin_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =

AM_CPPFLAGS = -I../../../c/include -I../../../bindings-common/include

include_HEADERS =
	
gsstubgen_SOURCES = \
	backendlib.c \
	c-backend.c \
	c-constraints-backend.c \
	corba-gram.y \
	corba-lex.l \
	main.c \
	semantic.c
