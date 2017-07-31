AM_YFLAGS = -d
ACLOCAL_AMFLAGS=-I m4 

bin_PROGRAMS = compss_generator
sbin_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =

AM_CPPFLAGS = -I../../../c/include -I../../../bindings-common/include

include_HEADERS =
	
compss_generator_SOURCES = \
	types.c \
	backendlib.c \
	c-backend.c \
	corba-gram.y \
	corba-lex.l \
	main.c \
	semantic.c
