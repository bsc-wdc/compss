AC_INIT
AM_INIT_AUTOMAKE(polylib, 5.22.1)
AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL

AC_LANG(C++)

AC_PATH_CLN(1.1.0, [
  LIBS="$LIBS $CLN_LIBS"
  CPPFLAGS="$CPPFLAGS $CLN_CPPFLAGS"
], AC_MSG_ERROR([No suitable installed version of CLN could be found.]))

AC_CONFIG_HEADERS(config.h)
AC_OUTPUT(Makefile polylib/Makefile)
