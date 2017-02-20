
bin_PROGRAMS = PACKAGE
ACLOCAL_AMFLAGS=-I m4

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


