# Copyright 2013 Barcelona Supercomputing Center (www.bsc.es)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

dnl Process this file with autoconf to produce a configure script.
AC_INIT()
AC_CONFIG_HEADER([config.h])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])

STUBGEN_MAJOR_VERSION=0
STUBGEN_MINOR_VERSION=0
STUBGEN_MICRO_VERSION=0

STUBGEN_VERSION=$STUBGEN_MAJOR_VERSION.$STUBGEN_MINOR_VERSION.$STUBGEN_MICRO_VERSION
AC_SUBST(STUBGEN_MAJOR_VERSION)
AC_SUBST(STUBGEN_MINOR_VERSION)
AC_SUBST(STUBGEN_MICRO_VERSION)

VERSION=${STUBGEN_VERSION}
PACKAGE=stubgen

AM_INIT_AUTOMAKE($PACKAGE, $VERSION)

dnl Checks for programs.
AC_PROG_YACC
AM_PROG_LEX

dnl Initialize maintainer mode
AM_MAINTAINER_MODE

dnl Checks for libraries.
AM_PATH_XML2(2.6.0,
	[
		CFLAGS="$CFLAGS $XML_CFLAGS"
		LIBS="$LIBS $XML_LIBS"
	],[
		AC_MSG_ERROR([libxml not found])
	]
)
	

dnl Checks for header files.
AC_FUNC_ALLOCA
AC_HEADER_STDC
AC_CHECK_HEADERS([limits.h malloc.h stdlib.h string.h strings.h unistd.h])

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

dnl Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STRTOD
AC_CHECK_FUNCS([strdup strrchr])

AC_OUTPUT(Makefile)
