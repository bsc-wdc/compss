m4_define([PACKAGE], [libcbindings])
m4_define([P_MAJOR_VERSION], [2])
m4_define([P_MINOR_VERSION], [2])
m4_define([P_MICRO_VERSION], [0])
m4_define([P_VERSION], [P_MAJOR_VERSION.P_MINOR_VERSION.P_MICRO_VERSION])
m4_define([SUPPORT_EMAIL], [support-compss@bsc.es])

AC_INIT(PACKAGE, [P_VERSION], [SUPPORT_EMAIL])

AC_PREREQ(2.61)
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR(.)

AM_INIT_AUTOMAKE([foreign])

AC_PROG_LIBTOOL

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdlib.h string.h])

AC_SUBST(JAVA_HOME)

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([strdup])

AC_OUTPUT(Makefile)
