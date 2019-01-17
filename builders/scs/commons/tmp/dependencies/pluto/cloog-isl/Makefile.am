#
#   /**-------------------------------------------------------------------**
#    **                              CLooG                                **
#    **-------------------------------------------------------------------**
#    **                           makefile.in                             **
#    **-------------------------------------------------------------------**
#    **                   First version: october 25th 2001                **
#    **-------------------------------------------------------------------**/
#
# makefile.in (or makefile if generated) of CLooG, the Chunky LOOp Generator.
# makefile.in is not a makefile, you must run the 'configure' shellscript to
# generate the makefile thanks to this file.

#/*****************************************************************************
# *               CLooG : the Chunky Loop Generator (experimental)            *
# *****************************************************************************
# *                                                                           *
# * Copyright (C) 2001 Cedric Bastoul                                         *
# *                                                                           *
# * This library is free software; you can redistribute it and/or             *
# * modify it under the terms of the GNU Lesser General Public                *
# * License as published by the Free Software Foundation; either              *
# * version 2.1 of the License, or (at your option) any later version.        *
# *                                                                           *
# * This library is distributed in the hope that it will be useful,           *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of            *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU         *
# * Lesser General Public License for more details.                           *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public          *
# * License along with this library; if not, write to the Free Software       *
# * Foundation, Inc., 51 Franklin Street, Fifth Floor,                        *
# * Boston, MA  02110-1301  USA                                               *
# *                                                                           *
# * CLooG, the Chunky Loop Generator                                          *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/
if BUNDLED_ISL
    MAYBE_ISL = isl
    ISL_LA = $(top_builddir)/isl/libisl.la
endif

if BUNDLED_OSL
    MAYBE_OSL = osl
    OSL_LA = $(top_builddir)/osl/libosl.la
endif

AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS = $(MAYBE_ISL) $(MAYBE_OSL) . doc test
DIST_SUBDIRS = $(MAYBE_ISL) $(MAYBE_OSL) . doc test

ACLOCAL_AMFLAGS = -I m4

FORCE:
isl/libisl.la: FORCE
	cd isl; $(MAKE) $(AM_MAKEFLAGS) libisl.la

if NO_ISL
CLOOG =
LIBCLOOG =
else
CLOOG = cloog
LIBCLOOG = libcloog-isl.la
endif
bin_PROGRAMS = $(CLOOG)
lib_LTLIBRARIES = $(LIBCLOOG)

if NEED_GET_MEMORY_FUNCTIONS
GET_MEMORY_FUNCTIONS=source/mp_get_memory_functions.c
endif

SOURCES_CORE = \
	$(GET_MEMORY_FUNCTIONS) \
	source/block.c \
	source/clast.c \
	source/matrix.c \
	source/state.c \
	source/input.c \
	source/int.c \
	source/loop.c \
	source/names.c \
	source/options.c \
	source/pprint.c \
	source/program.c \
	source/statement.c \
	source/stride.c \
	source/union_domain.c \
	source/util.c \
	source/version.c

AM_CPPFLAGS = -I. -I$(top_builddir)/include -I$(top_srcdir)/include
AM_CFLAGS = $(CFLAGS_WARN)
libcloog_isl_la_CPPFLAGS = -I. -I$(top_builddir)/include -I$(top_srcdir)/include @ISL_CPPFLAGS@ @OSL_CPPFLAGS@
libcloog_isl_la_LDFLAGS = -version-info @versioninfo@ \
	-rpath $(libdir) @ISL_LDFLAGS@ @OSL_LDFLAGS@
libcloog_isl_la_LIBADD = @ISL_LIBS@ @OSL_LIBS@ $(ISL_LA) $(OSL_LA)
libcloog_isl_la_SOURCES = \
	$(SOURCES_CORE) \
	source/isl/domain.c \
	source/isl/constraints.c \
	source/isl/backend.c
LDADD = libcloog-isl.la
cloog_DEPENDENCIES = libcloog-isl.la
cloog_SOURCES = source/cloog.c

pkginclude_HEADERS = \
	include/cloog/block.h \
	include/cloog/clast.h \
	include/cloog/cloog.h \
	include/cloog/input.h \
	include/cloog/int.h \
	include/cloog/matrix.h \
	include/cloog/state.h \
	include/cloog/domain.h \
	include/cloog/loop.h \
	include/cloog/constraints.h \
	include/cloog/names.h \
	include/cloog/options.h \
	include/cloog/pprint.h \
	include/cloog/program.h \
	include/cloog/statement.h \
	include/cloog/stride.h \
	include/cloog/union_domain.h \
	include/cloog/util.h \
	include/cloog/version.h

pkgmatrixincludedir = $(pkgincludedir)/matrix
pkgmatrixinclude_HEADERS = \
	include/cloog/matrix/constraintset.h

pkgislincludedir = $(pkgincludedir)/isl
pkgislinclude_HEADERS = \
	include/cloog/isl/backend.h \
	include/cloog/isl/cloog.h \
	include/cloog/isl/domain.h \
	include/cloog/isl/constraintset.h

version.h: @GIT_INDEX@
	echo '#define CLOOG_HEAD "'`$(top_builddir)/genversion.sh`'"' > $@

EXTRA_DIST = \
	autoconf/Doxyfile.in \
	source/matrix/constraintset.c \
	cmake \
	doc/cloog.texi \
	doc/images \
	examples

install-data-local:
	@test -z "$(pkgconfig_libdir)" || $(mkdir_p) "$(DESTDIR)$(pkgconfig_libdir)"
	$(INSTALL_DATA) $(pkgconfig_libfile) "$(DESTDIR)$(pkgconfig_libdir)/$(pkgconfig_libfile)"
	@test -z "$(DESTDIR)$(libdir)/isl" || $(mkdir_p) "$(DESTDIR)$(libdir)/isl"
	$(INSTALL_DATA) "$(top_srcdir)/cmake/isl-config.cmake" "$(DESTDIR)$(libdir)/isl/"
	@test -z "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)" || $(mkdir_p) "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)"
	$(INSTALL_DATA) "$(top_srcdir)/cmake/cloog-isl-config.cmake" "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)/"

uninstall-local:
	rm -f "$(DESTDIR)$(pkgconfig_libdir)/$(pkgconfig_libfile)"
	rm -f "$(DESTDIR)$(libdir)/isl/isl-config.cmake"
	rm -f "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)/cloog-isl-config.cmake"

dist-hook:
	rm -f $(distdir)/test/Makefile
	$(top_builddir)/genversion.sh > $(distdir)/CLOOG_HEAD
	(cd doc; make cloog.pdf) && cp doc/cloog.pdf $(distdir)/doc/

#/*****************************************************************************
# *                                   Rules                                   *
# *****************************************************************************/

valcheck:
	$(MAKE) test_valgrind -C test

test_hybrid:
	$(MAKE) test_hybrid -C test

total:
	@echo "             /*-----------------------------------------------*"
	@echo "              *                     CLooG                     *"
	@echo "              *-----------------------------------------------*/"
	$(MAKE) uninstall
	$(MAKE) clean
	$(MAKE)
	$(MAKE) install

doc:
	@echo "             /*-----------------------------------------------*"
	@echo "              *        Generating CLooG's documentation       *"
	@echo "              *-----------------------------------------------*/"
	doxygen ./autoconf/Doxyfile
