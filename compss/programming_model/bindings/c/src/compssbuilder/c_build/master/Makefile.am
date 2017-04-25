
bin_PROGRAMS = PACKAGE

ACLOCAL_AMFLAGS=-I m4

PACKAGE_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(CS_HOME)/../bindings-common/include -I$(CS_HOME)/include -w

PACKAGE_SOURCES = PACKAGE-stubs.cc \
        PACKAGE.cc

PACKAGE_LDFLAGS =

PACKAGE_LDADD = -L../../lib -lmaster -L$(JAVA_HOME)/jre/lib/$(ARCH)/$(SUBFLDR) -ljvm -ldl -L$(CS_HOME)/../bindings-common/lib -lbindings_common -L$(CS_HOME)/lib -lcbindings -lboost_iostreams -lboost_serialization

COMPSSGEN_FLAGS =

PACKAGE-executor.cc PACKAGE-stubs.cc PACKAGE.h: PACKAGE.idl
	$(COMPSSGEN) $(COMPSSGEN_FLAGS) PACKAGE.idl
	
