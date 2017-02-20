
bin_PROGRAMS = PACKAGE PACKAGE-worker

if USE_CLASSADS
classads_sources = PACKAGE_constraints_wrapper.cc PACKAGE_constraints.cc
else
classads_sources =
endif

ACLOCAL_AMFLAGS=-I m4

PACKAGE_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(GS_HOME)/../bindings-common/include -I$(GS_HOME)/include -w

PACKAGE_SOURCES = empty.cc \
        PACKAGE-stubs.cc \
        $(classads_sources) \
        PACKAGE.cc

PACKAGE_LDFLAGS =

PACKAGE_LDADD = -L../../lib -lmaster -L$(JAVA_HOME)/jre/lib/$(ARCH)/$(SUBFLDR) -ljvm -ldl -L$(GS_HOME)/../bindings-common/lib -lbindings_common -L$(GS_HOME)/lib -lcbindings -lboost_iostreams -lboost_serialization

PACKAGE_worker_SOURCES = \
        PACKAGE-worker.cc \
        PACKAGE-functions.cc
     
PACKAGE_worker_LDFLAGS =

PACKAGE_worker_LDADD = -L../../lib -lworker -ldl -lboost_iostreams -lboost_serialization

GSSTUBGEN_FLAGS =

PACKAGE-worker.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(GSSTUBGEN) $(GSSTUBGEN_FLAGS) PACKAGE.idl
	
