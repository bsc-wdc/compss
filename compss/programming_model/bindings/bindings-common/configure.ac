dnl Process this file with autoconf to produce a configure script.
m4_define([PACKAGE], [libbindings_common])
m4_define([P_MAJOR_VERSION], [2])
m4_define([P_MINOR_VERSION], [2])
m4_define([P_MICRO_VERSION], [0])
m4_define([P_VERSION], [P_MAJOR_VERSION.P_MINOR_VERSION.P_MICRO_VERSION])
m4_define([SUPPORT_EMAIL], [support-compss@bsc.es])

AC_INIT(PACKAGE, [P_VERSION], [SUPPORT_EMAIL])

AC_CONFIG_SRCDIR(src/GS_compss.cc)
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR([m4])                                                                                                                                                                                                                                                      

AM_INIT_AUTOMAKE([foreign])

AC_PROG_CXX

AC_PROG_LIBTOOL

AC_OUTPUT(Makefile src/Makefile)
