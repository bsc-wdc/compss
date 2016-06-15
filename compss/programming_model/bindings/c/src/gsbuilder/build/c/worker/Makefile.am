
bin_PROGRAMS = PACKAGE-worker

PACKAGE_worker_CPPFLAGS = -I../../src -I../../include -Wno-write-strings -I$(GS_HOME)/../bindings-common/include -I$../../../c/include -w

PACKAGE_worker_SOURCES = \
	PACKAGE-worker.cc \
	PACKAGE-functions.cc

PACKAGE_worker_LDFLAGS =

PACKAGE_worker_LDADD = -L../../lib -lworker -ldl -lboost_iostreams -lboost_serialization

GSSTUBGEN_FLAGS = -n


PACKAGE-worker.cc PACKAGE.h: PACKAGE.idl
	$(GSSTUBGEN) $(GSSTUBGEN_FLAGS) PACKAGE.idl


