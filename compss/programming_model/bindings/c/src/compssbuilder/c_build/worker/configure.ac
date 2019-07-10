
dnl Process this file with autoconf to produce a configure script.
AC_INIT()
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE(all, 0.0.0)

AC_CANONICAL_HOST

AC_MSG_CHECKING([if we should activate AIX workarounds])
case $host_os in
	aix*)
		AC_MSG_RESULT([yes])
		CC=xlc
		CXX=xlC
		LD=xlC
		LDFLAGS="-brtl -bmaxdata:0x70000000"
		IS_AIX=yes
		AC_SUBST(IS_AIX)
		break;
		;;
	*)
		AC_MSG_RESULT([no])
		break;
		;;
esac
dnl Common config script configuration variables

dnl Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AC_PROG_RANLIB

AM_PROG_CC_C_O

#AX_BOOST_BASE(1.54)
#AX_BOOST_SERIALIZATION

AC_PATH_PROG(COMPSSGEN, compss_generator, none)
if test x"$COMPSSGEN" = x"none" ; then
        AC_MSG_ERROR([Required compss_generator program missing.])
fi

dnl Initialize maintainer mode
AM_MAINTAINER_MODE
AC_ARG_WITH([cs-prefix],
	AS_HELP_STRING([  --with-cs-prefix=[prefix]], [Set the prefix under which the COMPSs Superscalar c library is installed.]),
	[
		LIBS=-L$withval/lib $LIBS
		CPPFLAGS="-I$withval/include $CPPFLAGS"
	]
)

ompss=false
ompss2=false
cuda=false

dnl Linking with Nanos++ libraries if necessary
AC_ARG_WITH([ompss],
    AS_HELP_STRING([ --with-ompss], [Enables the use of OmpSs and specificates its directory.]),
            [
                ompss=true
            ]
)

dnl Linking with Nanos6 libraries if necessary
AC_ARG_WITH([ompss-2],
    AS_HELP_STRING([ --with-ompss-2], [Enables the use of OmpSs2 and specificates its directory.]),
            [
                ompss2=true

                AC_CHECK_LIB([pthread], [pthread_cond_signal], [],
                [
                    AC_MSG_ERROR([Pthread library not found, is needed to enable OmpSs2.])
                ])

                AC_CHECK_LIB([dl], [dlopen], [],
                [
                    AC_MSG_ERROR([dl library not found, is needed to enable OmpSs2.])
                ])

                AC_CHECK_LIB([nanos6], [nanos6_spawn_function], [],
                [ 
                    AC_MSG_ERROR([Nanos6 library not found, is needed to enable OmpSs2.]) 
                ])

                AC_CHECK_LIB([custom-nanos6], [nanos6_library_mode_init], [],
                [
                    AC_MSG_ERROR([nanos6-library-mode.o is needed, we make a custom library from it.])
                ])
            ]
)

dnl Linking with CUDA libraries if necessary
AC_ARG_WITH([cuda],
    AS_HELP_STRING([ --with-cuda], [Enables the use of CUDA.]),
            [
                cuda=true
                
                AC_CHECK_LIB([cudart], [cudaMalloc], [],
                [
                    AC_MSG_ERROR([cudart library not found, is needed to enable CUDA.])
                ])
            ]
)

AM_CONDITIONAL([OMPSS], [test x$ompss = xtrue])
AM_CONDITIONAL([OMPSS2], [test x$ompss2 = xtrue])
AM_CONDITIONAL([CUDAOMPSS], [test x$cuda = xtrue && test x$ompss = xtrue])
AM_CONDITIONAL([CUDAOMPSS2], [test x$cuda = xtrue && test x$ompss2 = xtrue])

#Please, note that there are dependencies between the cuda option and OmpSs, OmpSs-2, in order to use cuda you must have enabled OmpSs or OmpSs-2. This dependencies are managed by the compss_build_app script.

dnl Checks for libraries.
#AC_CHECK_LIB(GS-master, GS_On,, [ AC_MSG_ERROR([Required Master COMP Supercalar c library cannot be found]) ])
#AC_CHECK_LIB(GS-worker, IniWorker,, [ AC_MSG_ERROR([Required Worker COMP Supercalar library cannot be found]) ])

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS(fcntl.h limits.h unistd.h)
#AC_CHECK_HEADERS(gs_base64.h, [], [AC_MSG_ERROR([COMP superscalar c header files could not be found])])

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_TYPE_SIGNAL

dnl Checks for library functions.
AC_CHECK_FUNCS(putenv socket strstr strtod)

AC_OUTPUT(Makefile)
