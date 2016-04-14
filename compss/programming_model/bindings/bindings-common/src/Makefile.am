
include_HEADERS = GS_compss.h param_metadata.h
lib_LTLIBRARIES = libbindings_common.la

libbindings_common_la_SOURCES = GS_compss.cc
libbindings_common_la_CPPFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux/ -I.
libbindings_common_la_LDFLAGS = -shared -L$(JAVA_LIB_DIR) -ljvm

ACLOCAL_AMFLAGS =-I m4

clean:
	rm -f *.o *.lo *~
