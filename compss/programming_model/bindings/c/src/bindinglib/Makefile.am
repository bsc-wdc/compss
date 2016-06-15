
include_HEADERS = GS_templates.h gs_base64.h
lib_LTLIBRARIES = libcbindings.la

libcbindings_la_SOURCES = GS_compss.cc base64.c
libcbindings_la_CPPFLAGS = -I../../../bindings-common/include ${BOOST_CPPFLAGS} $(DEFINE)
libcbindings_la_LDFLAGS = -L../../../bindings-common/lib -lbindings_common

ACLOCAL_AMFLAGS =-I m4

clean:
	rm -f *.o *.lo *~
