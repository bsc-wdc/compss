dnl
dnl   /**------- <| --------------------------------------------------------**
dnl    **         A                     Clan                                **
dnl    **---     /.\   -----------------------------------------------------**
dnl    **   <|  [""M#               configure.in                            **
dnl    **-   A   | #   -----------------------------------------------------**
dnl    **   /.\ [""M#         First version: 30/04/2008                     **
dnl    **- [""M# | #  U"U#U  -----------------------------------------------**
dnl         | #  | #  \ .:/
dnl         | #  | #___| #
dnl ******  | "--'     .-"  ***************************************************
dnl *     |"-"-"-"-"-#-#-##   Clan : the Chunky Loop Analyser (experimental)  *
dnl ****  |     # ## ######  **************************************************
dnl *      \       .::::'/                                                    *
dnl *       \      ::::'/     Copyright (C) 2008 University Paris-Sud 11      *
dnl *     :8a|    # # ##                                                      *
dnl *     ::88a      ###      This is free software; you can redistribute it  *
dnl *    ::::888a  8a ##::.   and/or modify it under the terms of the GNU     *
dnl *  ::::::::888a88a[]:::   Lesser General Public License as published by   *
dnl *::8:::::::::SUNDOGa8a::. the Free Software Foundation, either version    *
dnl *::::::::8::::888:Y8888::                2.1 of the License, or (at your  *
dnl *::::':::88::::888::Y88a::::::::::::...  option) any later version.       *
dnl *::'::..    .   .....   ..   ...  .                                       *
dnl * This software is distributed in the hope that it will be useful, but    *
dnl * WITHOUT ANY WARRANTY; without even the implied warranty of              *
dnl * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU       *
dnl * General Public License  for more details.	                              *
dnl *                                                                         *
dnl * You should have received a copy of the GNU Lesser General Public        *
dnl * License along with software; if not, write to the Free Software         *
dnl * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307    *
dnl * USA                                                                     *
dnl *                                                                         *
dnl * Clan, the Chunky Loop Analyser                                          *
dnl * Written by Cedric Bastoul, Cedric.Bastoul@u-psud.fr                     *
dnl *                                                                         *
dnl ***************************************************************************/
dnl
dnl Input file for autoconf to build a configuration shellscript.

m4_define([version_major], [0])
m4_define([version_minor], [8])
m4_define([version_revision], [1])

AC_PREREQ(2.53)

dnl AC_PROG_SED is only available from version 2.6 (released in 2003). CentosOS
dnl 5.10 still has an ancient version. The actual AC_PROG_SED macro does
dnl functional checks, but here
dnl we define a trivial local version for times when we are running on
dnl obsoldete autoconf.
ifdef([AC_PROG_SED], [],
      [AC_DEFUN([AC_PROG_SED], [AC_CHECK_PROG(SED, sed, sed)])])

dnl Fill here the @bug email adress.
AC_INIT([clan], [version_major.version_minor.version_revision],
        [clan-development@googlegroups.com])
dnl A common file, which serve as a test.
AC_CONFIG_SRCDIR([include/clan/symbol.h])
dnl Put as most as possible configuration files to an auxialiry
dnl directory.
AC_CONFIG_AUX_DIR(autoconf)
AC_CONFIG_MACRO_DIR([m4])

VERSION_MAJOR=version_major
VERSION_MINOR=version_minor
VERSION_REVISION=version_revision
AC_SUBST(versioninfo)
versioninfo=4:0:0

dnl Initialize automake.
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
AC_PROG_INSTALL
AC_PROG_LIBTOOL
AC_PROG_SED
AC_PROG_AWK
AC_PROG_LEX
AC_PROG_YACC

AC_CHECK_PROGS(DOXYGEN, doxygen, doxygen, [])
AC_CHECK_PROGS(TEXI2PDF, texi2pdf, texi2pdf, [])
AM_CONDITIONAL(GENERATE_DOC, test -n "$TEXI2PDF" -a -n "$DOXYGEN")

AC_MSG_CHECKING(flex version)
flex_good=1
if $LEX --version | grep "flex" >/dev/null; then
  flex_version=`$ac_cv_prog_LEX --version | sed 's/version//g' | awk '/flex/ {print $2}'`
  flex_major=`echo $flex_version| cut -d . -f 1`
  flex_minor=`echo $flex_version| cut -d . -f 2`
  flex_rev=`echo $flex_version| cut -d . -f 3`
   
  if test "$flex_major" -gt "2"; then
    flex_good=1
  else 
    if test "$flex_major" -eq "2" -a "$flex_minor" -gt "5"; then
    flex_good=1
    else 
      if test "$flex_major" -eq "2" -a "$flex_minor" -eq "5" -a "$flex_rev" -ge "35"; then
      flex_good=1
      else
      flex_good=0
      fi
    fi
  fi
else
  AC_MSG_ERROR([flex not found. Version 2.5.35 or greater is required.])
fi
if test $flex_good = 0; then
  AC_MSG_ERROR([flex version $flex_version found.
		Version 2.5.35 or greater is required.
		Please consider to upgrade flex.])
else
  AC_MSG_RESULT([$flex_version. OK])
fi

AC_MSG_CHECKING(bison version)
bison_good=1
if echo "$YACC" | grep bison > /dev/null; then
# NOTE: m4 removes [], that's why it needs to be escaped
  bison_version=[`$YACC --version | head -n 1 | sed -e 's/[^0-9.]*\([-0-9.]\+\)$/\1/'`]
  bison_version_major=`echo $bison_version | cut -d. -f1`
  bison_version_minor=`echo $bison_version | cut -d. -f2`
  if test "$bison_version_major" -lt 2; then
    bison_good=0
  else
    if test "$bison_version_major" -eq 2 -a "$bison_version_minor" -lt 4; then
      bison_good=0
    else
      bison_good=1
    fi
  fi
  if test $bison_good = 0; then
    AC_MSG_ERROR([bison version $bison_version found.
		  Version 2.4 or greater is required.
		  Please consider to upgrade bison.])
  else
    AC_MSG_RESULT([$bison_version. OK])
  fi
else
  AC_MSG_ERROR([bison not found. Version 2.4 or greater is required.])
fi

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([errno.h stddef.h stdlib.h string.h strings.h unistd.h])

dnl Checks for library functions.
AC_CHECK_FUNCS(strtol)


dnl /**************************************************************************
dnl  *                             Option setting                             *
dnl  **************************************************************************/

dnl /**************************************************************************
dnl  *                   Where is the OpenScop Library?                       *
dnl  **************************************************************************/

AX_SUBMODULE(osl,system|build|bundled,bundled)
AM_CONDITIONAL(BUNDLED_OSL, test $with_osl = bundled)

AC_SUBST(OSL_CFLAGS)
AC_SUBST(OSL_LIBS)
AC_SUBST(OSL_LDFLAGS)
case "$with_osl" in
bundled)
    OSL_CFLAGS="-I\$(top_srcdir)/osl/include -I\$(top_builddir)/osl/include"
    ;;
build)
    OSL_CFLAGS="-I$osl_srcdir/include -I$with_osl_builddir/include"
    OSL_LIBS="$with_osl_builddir/libosl.la"
    OSL_LDFLAGS="-L$with_osl_builddir"
    dnl ----- Check the headers (without polluting CPPFLAGS)
    save_CPPFLAGS=$CPPFLAGS
    CPPFLAGS="$CPPFLAGS $OSL_CFLAGS"
    AC_CHECK_HEADER(osl/osl.h, [], [AC_MSG_ERROR("Can't find osl headers.")])
    CPPFLAGS=$save_CPPFLAGS
    ;;
system)
    if test "x$with_osl_prefix" != "x"; then
	OSL_CFLAGS="-I$with_osl_prefix/include"
	OSL_LDFLAGS="-L$with_osl_exec_prefix/lib"
    fi
    if test "x$with_osl_exec_prefix" != "x"; then
	OSL_LDFLAGS="-L$with_osl_exec_prefix/lib"
    fi
    OSL_LIBS="-losl"
    dnl ----- Check the headers (without polluting CPPFLAGS)
    save_CPPFLAGS=$CPPFLAGS
    CPPFLAGS="$CPPFLAGS $OSL_CFLAGS"
    AC_CHECK_HEADER(osl/osl.h, [], [AC_MSG_ERROR("Can't find osl headers.")])
    CPPFLAGS=$save_CPPFLAGS
    dnl ----- Check the library (without polluting LDFLAGS)
    save_LDFLAGS=$LDFLAGS
    LDFLAGS="$LDFLAGS $OSL_LDFLAGS"
    AC_CHECK_LIB(osl, osl_scop_malloc, [], [AC_MSG_ERROR("Can't find osl library.")])
    LDFLAGS=$save_LDFLAGS
    ;;
esac


dnl /**************************************************************************
dnl  *                            Substitutions                               *
dnl  **************************************************************************/


dnl Substitutions to do.
AC_SUBST(ac_aux_dir)
AC_SUBST(abs_top_srcdir)

AC_SUBST(VERSION_MAJOR)
AC_SUBST(VERSION_MINOR)
AC_SUBST(VERSION_REVISION)

dnl Configure Makefiles.
AC_CONFIG_FILES([
	Makefile
	doc/Makefile
	doc/Doxyfile
	include/clan/macros.h
	tests/Makefile
	],
	[test -z "$CONFIG_HEADERS" || echo timestamp > source/stamp-h.in])

if test $with_osl = bundled; then
	AC_CONFIG_SUBDIRS(osl)
fi

AC_OUTPUT

echo "             /*-----------------------------------------------*"
echo "              *           Clan configuration is OK            *"
echo "              *-----------------------------------------------*/"
echo "It appears that your system is OK to start Clan compilation. You need"
echo "now to type \"make\". Lastly type \"make install\" to install Clan on"
echo "your system (log as root if necessary)."
