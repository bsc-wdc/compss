#
#  /**-------------------------------------------------------------------**
#   **                            PIP/PipLib                             **
#   **-------------------------------------------------------------------**
#   **                           makefile.in                             **
#   **-------------------------------------------------------------------**
#   **                   First version: july 28th 2001                   **
#   **-------------------------------------------------------------------**/
#
# Makefile.am (or Makefile if generated) of PIP/PipLib. Makefile.am is not a
# makefile, you must run the 'configure' shellscript to generate the Makefile
# thanks to this file. 

#############################################################################

AUTOMAKE_OPTIONS = subdir-objects
SUBDIRS = . example doc test
ACLOCAL_AMFLAGS = -I m4

#############################################################################

bin_PROGRAMS = @pips@
EXTRA_PROGRAMS = pip_sp pip_dp pip_gmp
lib_LTLIBRARIES = @piplibs@
EXTRA_LTLIBRARIES = libpiplib_sp.la libpiplib_dp.la libpiplib_gmp.la

#############################################################################

pkginclude_HEADERS = \
	include/piplib/piplib.h \
	include/piplib/piplib_sp.h \
	include/piplib/piplib_dp.h \
	include/piplib/piplib_gmp.h \
	include/piplib/piplib32.h \
	include/piplib/piplib64.h \
	include/piplib/piplibMP.h

LIBRARY_FILES = \
	source/integrer.c \
	source/piplib.c \
	source/sol.c \
	source/tab.c \
	source/traiter.c \
	source/funcall.h \
	source/pip.h \
	source/sol.h \
	source/tab.h \
	source/type.h

libpiplib_sp_la_SOURCES = $(LIBRARY_FILES)
libpiplib_sp_la_CPPFLAGS = @SP_DFLAGS@ -I$(srcdir)/include
libpiplib_sp_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpiplib_dp_la_SOURCES = $(LIBRARY_FILES)
libpiplib_dp_la_CPPFLAGS = @DP_DFLAGS@ -I$(srcdir)/include
libpiplib_dp_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpiplib_gmp_la_SOURCES = $(LIBRARY_FILES)
libpiplib_gmp_la_CPPFLAGS = @MP_DFLAGS@ -I$(srcdir)/include
libpiplib_gmp_la_LDFLAGS = @EXTRA_LIBS@ -rpath $(libdir) -version-info @versioninfo@

pip_sp_SOURCES = source/maind.c
pip_sp_CPPFLAGS = @SP_DFLAGS@ -I$(srcdir)/include
pip_sp_LDADD = libpiplib_sp.la
pip_dp_SOURCES = source/maind.c
pip_dp_CPPFLAGS = @DP_DFLAGS@ -I$(srcdir)/include
pip_dp_LDADD = libpiplib_dp.la
pip_gmp_SOURCES = source/maind.c
pip_gmp_CPPFLAGS = @MP_DFLAGS@ -I$(srcdir)/include
pip_gmp_LDADD = libpiplib_gmp.la @EXTRA_LIBS@

version.h: @GIT_INDEX@
	echo '#define PIPLIB_HEAD "'`$(top_builddir)/genversion.sh`'"' > $@

EXTRA_DIST = \
	autogen.sh \
	cmake/piplib32-config.cmake \
	cmake/piplib64-config.cmake \
	cmake/piplibmp-config.cmake \
	cmake/piplib_sp-config.cmake \
	cmake/piplib_dp-config.cmake \
	cmake/piplib_mp-config.cmake

install-data-local:
	@for conf in @cmakeconfigs@; do \
	  if [ ! -e "$(DESTDIR)$(libdir)/$$conf" ]; then $(mkdir_p) "$(DESTDIR)$(libdir)/$$conf"; fi;\
	  $(INSTALL_DATA) "$(top_srcdir)/cmake/$$conf-config.cmake" "$(DESTDIR)$(libdir)/$$conf/"; \
	done

uninstall-local:
	@for conf in @cmakeconfigs@; do \
	  rm -f "$(DESTDIR)$(libdir)/$$conf/$$conf-config.cmake"; \
	done

dist-hook:
	(cd doc; make piplib.pdf) && cp doc/piplib.pdf $(distdir)/doc/
	$(top_builddir)/genversion.sh > $(distdir)/PIPLIB_HEAD
	
check:
	@for pip in @pips@; do \
	    (cd test; $(MAKE) test PIP=$$pip); \
	done
	@for example in @examples@; do \
	    (cd example; $(MAKE) test  EXAMPLE=$$example); \
	done
	
total:
	@echo "             /*-----------------------------------------------*"
	@echo "              *                   PIP/PipLib                  *"
	@echo "              *-----------------------------------------------*/"
	$(MAKE) uninstall
	$(MAKE) clean
	$(MAKE)
	$(MAKE) install
