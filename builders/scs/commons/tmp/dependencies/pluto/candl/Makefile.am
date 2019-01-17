#
#   /**-------------------------------------------------------------------**
#    **                              CAnDL                                **
#    **-------------------------------------------------------------------**
#    **                           Makefile.am                             **
#    **-------------------------------------------------------------------**
#    **                 First version: september 8th 2003                 **
#    **-------------------------------------------------------------------**/
#
#/*****************************************************************************
# *   CAnDL : the Chunky Analyser for Dependences in Loops (experimental)     *
# *****************************************************************************
# *                                                                           *
# * Copyright (C) 2003-2008 Cedric Bastoul                                    *
# *                                                                           *
# * This is free software; you can redistribute it and/or modify it under the *
# * terms of the GNU Lesser General Public License as published by the Free   *
# * Software Foundation; either version 3 of the License, or (at your option) *
# * any later version.							      *
# *                                                                           *
# * This software is distributed in the hope that it will be useful, but      *
# * WITHOUT ANY WARRANTY; without even the implied warranty of                *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General *
# * Public License for more details.                                          *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public License  *
# * along with software; if not, write to the Free Software Foundation, Inc., *
# * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                    *
# *                                                                           *
# * CAnDL, the Chunky Dependence Analyser                                     *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/

#############################################################################

if BUNDLED_OSL
  MAYBE_OSL = osl
  OSL_LA = $(top_builddir)/osl/libosl.la
endif
if BUNDLED_PIPLIB
  MAYBE_PIPLIB = piplib
  ifeq ($(BITS),32)
    PIPLIB_LA = $(top_builddir)/piplib/libpiplib_sp.la
  endif
  ifeq ($(BITS),64)
    PIPLIB_LA = $(top_builddir)/piplib/libpiplib_dp.la
  endif
  ifeq ($(BITS),MP)
    PIPLIB_LA = $(top_builddir)/piplib/libpiplib_gmp.la
  endif
endif

AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS         = $(MAYBE_OSL) $(MAYBE_PIPLIB) . doc tests
DIST_SUBDIRS    = $(MAYBE_OSL) $(MAYBE_PIPLIB) . doc tests
ACLOCAL_AMFLAGS = -I m4

#############################################################################

bin_PROGRAMS    = candl
lib_LTLIBRARIES = libcandl.la

#############################################################################

pkginclude_HEADERS =                   \
	include/candl/candl.h          \
	include/candl/dependence.h     \
	include/candl/scop.h           \
	include/candl/statement.h      \
	include/candl/label_mapping.h  \
	include/candl/macros.h         \
	include/candl/util.h           \
	include/candl/ddv.h            \
	include/candl/matrix.h         \
	include/candl/options.h        \
	include/candl/piplib.h         \
	include/candl/piplib-wrapper.h \
	include/candl/violation.h

DEFAULT_CPPFLAGS = -I.
AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
AM_CFLAGS = $(CFLAGS_WARN)

#############################################################################

EXTRA_DIST = COPYING.LESSER AUTHORS candl-config.cmake

libcandl_la_LIBADD   = @OSL_LIBS@ @PIPLIB_LIBS@ $(OSL_LA) $(PIPLIB_LA)
libcandl_la_CPPFLAGS = -I. -I$(top_builddir)/include -I$(top_srcdir)/include \
		       @OSL_CPPFLAGS@ @PIPLIB_CPPFLAGS@
libcandl_la_LDFLAGS  = @OSL_LDFLAGS@ @PIPLIB_LDFLAGS@ 
libcandl_la_SOURCES  =          \
	source/dependence.c     \
	source/ddv.c            \
	source/scop.c           \
	source/statement.c      \
	source/util.c           \
	source/isl-wrapper.c    \
	source/label_mapping.c  \
	source/matrix.c         \
	source/options.c        \
	source/piplib-wrapper.c \
	source/pruning.c        \
	source/violation.c

#############################################################################

LDADD              = @CANDL_LIBS@ @OSL_LIBS@ @PIPLIB_LIBS@
candl_CPPFLAGS     = -I. -I$(top_builddir)/include -I$(top_srcdir)/include \
		     @OSL_CPPFLAGS@ @PIPLIB_CPPFLAGS@ -Wall
candl_LDFLAGS      = #@OSL_LDFLAGS@ @PIPLIB_LDFLAGS@ # TO BE REMOVED
candl_DEPENDENCIES = libcandl.la
candl_SOURCES      = source/candl.c

#############################################################################

MAINTAINERCLEANFILES = \
	Makefile.in    \
	aclocal.m4     \
	configure      \
	$(AUX_DIST)

#############################################################################
install-data-local:
	@test -z "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)" || $(mkdir_p) "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)"
	$(INSTALL_DATA) "$(top_srcdir)/candl-config.cmake" "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)/"

uninstall-local:
	rm -f "$(DESTDIR)$(libdir)/$(PACKAGE_NAME)/candl-config.cmake"

dist-hook:
	(cd $(distdir) && mkdir -p $(ac_aux_dir))
	for file in $(AUX_DIST); do \
	  cp $$file $(distdir)/$$file; \
	done
#############################################################################

valcheck:
	$(MAKE) valcheck -C tests
