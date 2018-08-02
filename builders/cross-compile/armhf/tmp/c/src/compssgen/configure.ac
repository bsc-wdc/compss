dnl Process this file with autoconf to produce a configure script.

m4_define([PACKAGE], [compssgen])
m4_define([P_MAJOR_VERSION], [2])
m4_define([P_MINOR_VERSION], [2])
m4_define([P_MICRO_VERSION], [0])
m4_define([P_VERSION], [P_MAJOR_VERSION.P_MINOR_VERSION.P_MICRO_VERSION])
m4_define([SUPPORT_EMAIL], [support-compss@bsc.es])

AC_INIT(PACKAGE, [P_VERSION], [SUPPORT_EMAIL])

AC_CONFIG_HEADER([config.h])
AC_PROG_LIBTOOL
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([foreign])

dnl Checks for programs.
AC_PROG_YACC
AM_PROG_LEX

dnl Initialize maintainer mode
AM_MAINTAINER_MODE

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
