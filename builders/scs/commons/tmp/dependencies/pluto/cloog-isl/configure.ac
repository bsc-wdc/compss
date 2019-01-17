

dnl /**-------------------------------------------------------------------**
dnl  **                              CLooG                                **
dnl  **-------------------------------------------------------------------**
dnl  **                           configure.ac                            **
dnl  **-------------------------------------------------------------------**
dnl  **                   First version: august 7th 2002                  **
dnl  **-------------------------------------------------------------------**/
dnl
dnl Input file for autoconf to build a configuration shellscript.
dnl To build the configure script from the CLooG's top-level directory, use
dnl autoconf -l autoconf autoconf/configure.in > configure
dnl if it doesn't work (invalid option -l) try -I instead
dnl autoconf -I autoconf autoconf/configure.in > configure

dnl /**************************************************************************
dnl *               CLooG : the Chunky Loop Generator (experimental)          *
dnl ***************************************************************************
dnl *                                                                         *
dnl * Copyright (C) 2001 Cedric Bastoul                                       *
dnl *                                                                         *
dnl * This library is free software; you can redistribute it and/or           *
dnl * modify it under the terms of the GNU Lesser General Public              *
dnl * License as published by the Free Software Foundation; either            *
dnl * version 2.1 of the License, or (at your option) any later version.      *
dnl *                                                                         *
dnl * This library is distributed in the hope that it will be useful,         *
dnl * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
dnl * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU       *
dnl * Lesser General Public License for more details.                         *
dnl *                                                                         *
dnl * You should have received a copy of the GNU Lesser General Public        *
dnl * License along with this library; if not, write to the Free Software     *
dnl * Foundation, Inc., 51 Franklin Street, Fifth Floor,                      *
dnl * Boston, MA  02110-1301  USA                                             *
dnl *                                                                         *
dnl * CLooG, the Chunky Loop Generator                                        *
dnl * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                      *
dnl *                                                                         *
dnl ***************************************************************************/

m4_define([version_major], [0])
m4_define([version_minor], [18])
m4_define([version_revision], [5])

AC_PREREQ(2.53)
AC_INIT([cloog], [version_major.version_minor.version_revision],
	[cloog-development@googlegroups.com])
AC_CONFIG_SRCDIR(source/cloog.c)
AC_CONFIG_AUX_DIR(autoconf)
AC_CONFIG_MACRO_DIR([m4])

VERSION_MAJOR=version_major
VERSION_MINOR=version_minor
VERSION_REVISION=version_revision
AC_SUBST(versioninfo)
versioninfo=4:0:0

AM_INIT_AUTOMAKE([foreign])
m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

dnl /**************************************************************************
dnl  *                              Checking                                  *
dnl  **************************************************************************/

dnl Checks for programs.
AC_PROG_CC
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_CHECK_PROG(CD, cd)
dnl Configure needs an empty install.sh file with this, i HATE that...
AC_PROG_INSTALL
AC_PROG_LIBTOOL
AC_CHECK_PROG(TEXI2DVI, texi2dvi, texi2dvi, [])
AM_CONDITIONAL(HAVE_TEXI2DVI, test -n "$TEXI2DVI")

AX_CC_MAXOPT
AC_SUBST(CFLAGS_WARN)
AX_CFLAGS_WARN_ALL(CFLAGS_WARN)

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T



dnl Checks for header files.
AC_HEADER_STDC


dnl Checks for library functions.
AC_CHECK_FUNCS(strtol)


dnl /**************************************************************************
dnl  *                             Option setting                             *
dnl  **************************************************************************/

AC_CHECK_FUNCS([getrusage],
	[AC_DEFINE([CLOOG_RUSAGE], [], [Print time required to generate code])])

AX_SUBMODULE(isl,no|system|build|bundled,bundled)

dnl /**************************************************************************
dnl  *                            Where is GMP?                               *
dnl  **************************************************************************/

AC_ARG_WITH(cloog-int,
            [AS_HELP_STRING(
              [--with-cloog-int=gmp|int|long|longlong],
              [Integer precision to use (default gmp)])])

if test "x$with_cloog_int" = "x"; then
  with_cloog_int="gmp"
fi

AX_SUBMODULE(gmp,system|build,system)

need_get_memory_functions=false
case "$with_cloog_int" in
  gmp)
    BITS="gmp"
    CLOOG_INT_CONFIGURE="CLOOG_INT_GMP"
    case "$with_gmp" in
    build)
      CPPFLAGS="-I$with_gmp_builddir $CPPFLAGS"
      LDFLAGS="-L$with_gmp_builddir/$lt_cv_objdir $LDFLAGS"
    ;;
    system)
      if test "x$with_gmp_prefix" != "x"; then
        CPPFLAGS="-I$with_gmp_prefix/include $CPPFLAGS"
      fi

      if test "$with_gmp_exec_prefix" != "yes" ; then
        LDFLAGS="-L$with_gmp_exec_prefix/lib $LDFLAGS"
      fi
    ;;
    esac
    case "$with_gmp" in
    build|system)
      AC_CHECK_HEADER(gmp.h,
                      [],
                      [AC_MSG_ERROR(Can't find gmp headers.)])
      AC_CHECK_LIB(gmp,
                  __gmpz_init,
                  [LIBS="$LIBS -lgmp"],
                  [AC_MSG_ERROR(Can't find gmp library.)])
      AC_CHECK_DECLS(mp_get_memory_functions,[],[
        need_get_memory_functions=true
      ],[#include <gmp.h>])
    ;;
    esac
  ;;
  int)
    BITS="int"
    CLOOG_INT_CONFIGURE="CLOOG_INT_INT"
  ;;
  long)
    BITS="long"
    CLOOG_INT_CONFIGURE="CLOOG_INT_LONG"
  ;;
  longlong)
    BITS="longlong"
    CLOOG_INT_CONFIGURE="CLOOG_INT_LONG_LONG"
  ;;
  *)
    AC_MSG_ERROR([Unknown integer type: $with_cloog_int])
  ;;
esac

AM_CONDITIONAL(NEED_GET_MEMORY_FUNCTIONS,
		test x$need_get_memory_functions = xtrue)

dnl /**************************************************************************
dnl  *                          Where is isl?                                 *
dnl  **************************************************************************/

AC_SUBST(ISL_CPPFLAGS)
AC_SUBST(ISL_LDFLAGS)
AC_SUBST(ISL_LIBS)
case "$with_isl" in
bundled)
	ISL_CPPFLAGS="-I$srcdir/isl/include -Iisl/include"
  CPPFLAGS="-DISL_USING_GMP_VAL=1 $CPPFLAGS"
	;;
build)
  TMPCPPFLAGS="$CPPFLAGS"
  CPPFLAGS="-I$isl_srcdir/include -I$with_isl_builddir/include $CPPFLAGS"
  AC_CHECK_HEADER([isl/val_gmp.h])
  CPPFLAGS="$TMPCPPFLAGS"
  if test "$ac_cv_header_isl_val_gmp_h" = "yes"; then
    CPPFLAGS="-DISL_USING_GMP_VAL=1 $CPPFLAGS"
  else
    CPPFLAGS="-DISL_USING_GMP_VAL=0 $CPPFLAGS"
  fi
	ISL_CPPFLAGS="-I$isl_srcdir/include -I$with_isl_builddir/include"
	ISL_LIBS="$with_isl_builddir/libisl.la"
	;;
system)
  TMPCPPFLAGS="$CPPFLAGS"
  CPPFLAGS="-I$with_isl_prefix/include $CPPFLAGS"
  AC_CHECK_HEADER([isl/val_gmp.h])
  CPPFLAGS="$TMPCPPFLAGS"
  if test "$ac_cv_header_isl_val_gmp_h" = "yes"; then
    CPPFLAGS="-DISL_USING_GMP_VAL=1 $CPPFLAGS"
  else
    CPPFLAGS="-DISL_USING_GMP_VAL=0 $CPPFLAGS"
  fi
	if test "x$with_isl_prefix" != "x"; then
		ISL_CPPFLAGS="-I$with_isl_prefix/include"
	fi
	if test "x$with_isl_exec_prefix" != "x"; then
		ISL_LDFLAGS="-L$with_isl_exec_prefix/lib"
	fi
	ISL_LIBS="-lisl"
esac


AM_CONDITIONAL(BUNDLED_ISL, test $with_isl = bundled)
AM_CONDITIONAL(NO_ISL, test $with_isl = no)


dnl /**************************************************************************
dnl  *                   Where is the OpenScop Library?                       *
dnl  **************************************************************************/

AX_SUBMODULE(osl,no|system|build|bundled,no)
osl_flag="OSL_SUPPORT"

AC_SUBST(OSL_CPPFLAGS)
AC_SUBST(OSL_LDFLAGS)
AC_SUBST(OSL_LIBS)
case "$with_osl" in
bundled)
	OSL_CPPFLAGS="-D$osl_flag -I$srcdir/osl/include -Iosl/include"
	;;
build)
	OSL_CPPFLAGS="-D$osl_flag -I$osl_srcdir/include -I$with_osl_builddir/include"
	OSL_LIBS="$with_osl_builddir/libosl.la"
	;;
system)
	if test "x$with_osl_prefix" != "x"; then
		OSL_CPPFLAGS="-I$with_osl_prefix/include"
	fi
	if test "x$with_osl_exec_prefix" != "x"; then
		OSL_LDFLAGS="-L$with_osl_exec_prefix/lib"
	fi
	OSL_LIBS="-losl"
	OSL_CPPFLAGS+=" -D$osl_flag"
esac
AM_CONDITIONAL(BUNDLED_OSL, test $with_osl = bundled)
AM_CONDITIONAL(NO_OSL, test $with_osl = no)

AC_SUBST(GIT_INDEX)
if test -f $srcdir/.git/HEAD; then
	GIT_INDEX="\$(top_srcdir)/.git/index"
fi


dnl /**************************************************************************
dnl  *                            Substitutions                               *
dnl  **************************************************************************/


dnl Substitutions to do in Makefile.in.
AC_SUBST(CC)
AC_SUBST(LN_S)

AC_SUBST(prefix)
AC_SUBST(exec_prefix)
AC_SUBST(INSTALL)

AC_SUBST(BITS)
AC_SUBST(CLOOG_INT_CONFIGURE)

AC_SUBST(VERSION_MAJOR)
AC_SUBST(VERSION_MINOR)
AC_SUBST(VERSION_REVISION)

PACKAGE_NAME="cloog-isl"
AX_CREATE_PKGCONFIG_INFO

AC_CONFIG_FILES(Makefile test/Makefile)
AC_CONFIG_FILES(autoconf/Doxyfile)
AC_CONFIG_FILES(doc/Makefile)
AC_CONFIG_FILES(source/version.c)
AC_CONFIG_FILES(include/cloog/int.h)
AC_CONFIG_FILES(include/cloog/version.h)
AC_CONFIG_FILES([genversion.sh], [chmod +x genversion.sh])
AC_CONFIG_COMMANDS([version.h],
	[echo '#define CLOOG_HEAD "'`./genversion.sh`'"' > version.h])
AC_CONFIG_COMMANDS([doc/gitversion.texi],
	[echo '@set VERSION '`./genversion.sh`'' > doc/gitversion.texi])
if test $with_isl = bundled; then
	AC_CONFIG_SUBDIRS(isl)
fi
if test $with_osl = bundled; then
	AC_CONFIG_SUBDIRS(osl)
fi
AC_CONFIG_COMMANDS_POST([
	ac_configure_args="$ac_configure_args $cloog_configure_args"
])
AC_OUTPUT


echo "             /*-----------------------------------------------*"
echo "              *           CLooG configuration is OK           *"
echo "              *-----------------------------------------------*/"
echo "It appears that your system is OK to start CLooG compilation. You need"
echo "now to type \"make\". After compilation, you should check CLooG by typing"
echo "\"make check\". If no problem occur, you can type \"make uninstall\" if"
echo "you are upgrading an old version. Lastly type \"make install\" to install"
echo "CLooG on your system (log as root if necessary)."
