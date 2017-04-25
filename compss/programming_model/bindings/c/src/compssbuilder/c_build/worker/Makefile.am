
bin_PROGRAMS = worker_c persistent_worker_c

ACLOCAL_AMFLAGS=-I m4

worker_c_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -w

worker_c_SOURCES = worker_c.cc \
        PACKAGE-executor.cc \
        PACKAGE-functions.cc
     
worker_c_LDFLAGS =

worker_c_LDADD = -L../../lib -lworker -ldl -lboost_iostreams -lboost_serialization

persistent_worker_c_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -w

persistent_worker_c_SOURCES = persistent_worker_c.cc \
        PACKAGE-executor.cc \
        PACKAGE-functions.cc
        
persistent_worker_c_LDFLAGS =

persistent_worker_c_LDADD = -L../../lib -lworker -ldl -lboost_iostreams -lboost_serialization

COMPSSGEN_FLAGS =

PACKAGE-executor.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(COMPSSGEN) $(COMPSSGEN_FLAGS) PACKAGE.idl
	
