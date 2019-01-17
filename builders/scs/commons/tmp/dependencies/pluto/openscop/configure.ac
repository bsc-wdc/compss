dnl
dnl  /*+------------------------------------------------------------------**
dnl   **                       OpenScop Library                           **
dnl   **------------------------------------------------------------------**
dnl   **                         configure.in                             **
dnl   **------------------------------------------------------------------**
dnl   **                   First version: 30/04/2008                      **
dnl   **------------------------------------------------------------------**
dnl
dnl
dnl **************************************************************************
dnl * OpenScop: Structures and formats for polyhedral tools to talk together *
dnl **************************************************************************
dnl *   ,___,,_,__,,__,,__,,__,,_,__,,_,__,,__,,___,_,__,,_,__,              *
dnl *   /   / /  //  //  //  // /   / /  //  //   / /  // /  /|,_,           *
dnl *  /   / /  //  //  //  // /   / /  //  //   / /  // /  / / /\           *
dnl * |~~~|~|~~~|~~~|~~~|~~~|~|~~~|~|~~~|~~~|~~~|~|~~~|~|~~~|/_/  \          *
dnl * | G |C| P | = | L | P |=| = |C| = | = | = |=| = |=| C |\  \ /\         *
dnl * | R |l| o | = | e | l |=| = |a| = | = | = |=| = |=| L | \# \ /\        *
dnl * | A |a| l | = | t | u |=| = |n| = | = | = |=| = |=| o | |\# \  \       *
dnl * | P |n| l | = | s | t |=| = |d| = | = | = | |   |=| o | | \# \  \      *
dnl * | H | | y |   | e | o | | = |l|   |   | = | |   | | G | |  \  \  \     *
dnl * | I | |   |   | e |   | |   | |   |   |   | |   | |   | |   \  \  \    *
dnl * | T | |   |   |   |   | |   | |   |   |   | |   | |   | |    \  \  \   *
dnl * | E | |   |   |   |   | |   | |   |   |   | |   | |   | |     \  \  \  *
dnl * | * |*| * | * | * | * |*| * |*| * | * | * |*| * |*| * | /      \* \  \ *
dnl * | O |p| e | n | S | c |o| p |-| L | i | b |r| a |r| y |/        \  \ / *
dnl * '---'-'---'---'---'---'-'---'-'---'---'---'-'---'-'---'          '--'  *
dnl *                                                                        *
dnl * Copyright (C) 2008 University Paris-Sud 11 and INRIA                   *
dnl *                                                                        *
dnl * (3-clause BSD license)                                                 *
dnl * Redistribution and use in source  and binary forms, with or without    *
dnl * modification, are permitted provided that the following conditions     *
dnl * are met:                                                               *
dnl *                                                                        *
dnl * 1. Redistributions of source code must retain the above copyright      *
dnl *    notice, this list of conditions and the following disclaimer.       *
dnl * 2. Redistributions in binary form must reproduce the above copyright   *
dnl *    notice, this list of conditions and the following disclaimer in the *
dnl *    documentation and/or other materials provided with the distribution.*
dnl * 3. The name of the author may not be used to endorse or promote        *
dnl *    products derived from this software without specific prior written  *
dnl *    permission.                                                         *
dnl *                                                                        *
dnl * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR   *
dnl * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED         *
dnl * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE *
dnl * DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,     *
dnl * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES     *
dnl * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR     *
dnl * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)     *
dnl * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,    *
dnl * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING  *
dnl * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE     *
dnl * POSSIBILITY OF SUCH DAMAGE.                                            *
dnl *                                                                        *
dnl * OpenScop Library, a library to manipulate OpenScop formats and data    *
dnl * structures. Written by:                                                *
dnl * Cedric Bastoul     <Cedric.Bastoul@u-psud.fr> and                      *
dnl * Louis-Noel Pouchet <Louis-Noel.pouchet@inria.fr>                       *
dnl *                                                                        *
dnl **************************************************************************/
dnl
dnl Input file for autoconf to build a configuration shellscript.

m4_define([version_major], [0])
m4_define([version_minor], [9])
m4_define([version_revision], [2])

AC_PREREQ(2.53)
dnl Fill here the @bug email adress.
AC_INIT([osl], [version_major.version_minor.version_revision],
        [cedric.bastoul@u-psud.fr,pouchet@cse.ohio-state.edu])
dnl A common file, which serve as a test.
AC_CONFIG_SRCDIR([include/osl/macros.h])
dnl Put as most as possible configuration files to an auxialiry
dnl directory.
AC_CONFIG_AUX_DIR([autoconf])
AC_CONFIG_MACRO_DIR([m4])

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

AC_CHECK_PROGS(DOXYGEN, doxygen, doxygen, [])
AC_CHECK_PROGS(TEXI2PDF, texi2pdf, texi2pdf, [])
AM_CONDITIONAL(GENERATE_DOC, test -n "$TEXI2PDF" -a -n "$DOXYGEN")
  
AX_CC_MAXOPT
AC_SUBST(CFLAGS_WARN)
AX_CFLAGS_WARN_ALL(CFLAGS_WARN)

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
dnl  *                            Where is GMP?                               *
dnl  **************************************************************************/

gmp_flag="OSL_GMP_IS_HERE"
AX_SUBMODULE(gmp,no|system|build,system)

case "$with_gmp" in
build)
    CPPFLAGS="-D$gmp_flag -I$gmp_srcdir -I$with_gmp_builddir $CPPFLAGS"
    LIBS="$with_gmp_builddir/libgmp.la $LIBS"
    LDFLAGS="-L$with_gmp_builddir $LDFLAGS"
    ;;
system)
    CPPFLAGS="-D$gmp_flag $CPPFLAGS"
    LIBS="-lgmp $LIBS"
    if test "x$with_gmp_prefix" != "x"; then
	CPPFLAGS="-I$with_gmp_prefix/include $CPPFLAGS"
	LDFLAGS="-L$with_gmp_prefix/lib $LDFLAGS"
    fi
    dnl AC_CHECK_LIB doesn't use LDFLAGS --> not suitable for the build case.
    AC_CHECK_LIB(gmp,
                 __gmpz_init,
                 [],
                 [AC_MSG_ERROR("Can't find gmp library.")])
    ;;
esac
case "$with_gmp" in
build|system)
    AC_CHECK_HEADER(gmp.h,
                    [],
                    [AC_MSG_ERROR("Can't find gmp headers.")])
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
	doc/Doxyfile
	include/osl/scop.h
	],
	[test -z "$CONFIG_HEADERS" || echo timestamp > source/stamp-h.in])

AC_OUTPUT


echo "             /*-----------------------------------------------*"
echo "              *      OpenScop Library configuration is OK     *"
echo "              *-----------------------------------------------*/"
echo "It appears that your system is OK to start the OpenScop Library compilation."
echo "You need now to type \"make\". Then type \"make install\" to install it on your"
echo "system (log as root if necessary)."
