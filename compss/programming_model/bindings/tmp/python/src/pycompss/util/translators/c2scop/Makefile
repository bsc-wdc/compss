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

OPENSCOP_INC = /opt/COMPSs/Dependencies/pluto/include/osl
OPENSCOP_LIB = /opt/COMPSs/Dependencies/pluto/lib64/osl

CLAN_INC = /opt/COMPSs/Dependencies/pluto/include
CLAN_LIB = /opt/COMPSs/Dependencies/pluto/lib64

CC = gcc
LDLIBS= -losl -lclan
CFLAGS= -I $(OPENSCOP_INC) -L $(OPENSCOP_LIB) \
        -I $(CLAN_INC)     -L $(CLAN_LIB)

compile: translator_c2scop.c
	@echo "Compiling Translator"
	$(CC) translator_c2scop.c -o translator_c2scop $(CFLAGS) $(LDLIBS)

tests: test1 test2 test3

test1: compile
	@echo "Executing Translator for Test 1: Matmul"
	export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(OPENSCOP_LIB):$(CLAN_LIB); ./translator_c2scop ./tests/test1_matmul.src.c ./tests/test1_matmul.out.scop
	diff ./tests/test1_matmul.out.scop ./tests/test1_matmul.expected.scop
	@echo "PASS"

test2: compile
	@echo "Executing Translator for Test 2: Seidel"
	export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(OPENSCOP_LIB):$(CLAN_LIB); ./translator_c2scop ./tests/test2_seidel.src.c ./tests/test2_seidel.out.scop
	diff ./tests/test2_seidel.out.scop ./tests/test2_seidel.expected.scop
	@echo "PASS"

test3: compile
	@echo "Executing Translator for Test 3: FDTD-1D"
	export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(OPENSCOP_LIB):$(CLAN_LIB); ./translator_c2scop ./tests/test3_fdtd-1d.src.c ./tests/test3_fdtd-1d.out.scop
	diff ./tests/test3_fdtd-1d.out.scop ./tests/test3_fdtd-1d.expected.scop
	@echo "PASS"

clean:
	@echo "Cleaning folder"
	-rm -f translator_c2scop core ./tests/test1_matmul.out.scop ./tests/test2_seidel.out.scop ./tests/test3_fdtd-1d.out.scop

