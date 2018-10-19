dnl Process this file with autoconf to produce a configure script.

m4_define([PACKAGE], [compssbuilder])
m4_define([P_MAJOR_VERSION], [2])
m4_define([P_MINOR_VERSION], [2])
m4_define([P_MICRO_VERSION], [0])
m4_define([P_VERSION], [P_MAJOR_VERSION.P_MINOR_VERSION.P_MICRO_VERSION])
m4_define([SUPPORT_EMAIL], [support-compss@bsc.es])

AC_INIT(PACKAGE, [P_VERSION], [SUPPORT_EMAIL])

AM_INIT_AUTOMAKE([foreign])
AC_PROG_LIBTOOL
#AC_CONFIG_SUBDIRS([build/c/all build/c/master build/c/worker])
AC_CONFIG_MACRO_DIR([m4])

AC_CANONICAL_HOST
 
AC_MSG_CHECKING([if we should activate AIX workarounds])
case $host_os in
	aix*)
		AC_MSG_RESULT([yes])
		CC=xlc
		CXX=xlC
		LD=xlc
		LDFLAGS=-bmaxdata:0x70000000
		IS_AIX=yes
		AC_SUBST(IS_AIX)
		break;
		;;
	*)
		AC_MSG_RESULT([no])
		break;
		;;
esac

dnl Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LD
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PATH_PROG([LD], [$LD], [none])
AC_PATH_PROG([CHMOD], [chmod], [none])
AC_PATH_PROG([AUTOHEADER], [autoheader], [none])
AC_PATH_PROG([ACLOCAL], [aclocal], [none])
AC_PATH_PROG([AUTOMAKE], [automake], [none])
AC_PATH_PROG([AUTOCONF], [autoconf], [none])
AC_PATH_PROG([AUTORECONF], [autoreconf], [none])
AC_PATH_PROG([LIBTOOLIZE], [libtoolize], [none])
AC_PATH_PROG([MAKE], [make], [none])
AC_PATH_PROG([CP], [cp], [none])
AC_PATH_PROG([CAT], [cat], [none])
AC_PATH_PROG([BASH], [bash], [none])
AC_PATH_PROG([RM], [rm], [none])
AC_PATH_PROG([MKDIR], [mkdir], [none])
AC_PATH_PROG([MV], [mv], [none])
AC_PATH_PROG([TOUCH], [touch], [none])
AC_PATH_PROG([SED], [sed], [none])

#AX_BOOST_BASE(1.54)
#AX_BOOST_SERIALIZATION

#AC_CONFIG_LINKS([test.txt:test.txt])

for cmd in $CC $LD $CHMOD $CAT $RM $CP $BASH $MKDIR $MV $TOUCH $AUTOHEADER $ACLOCAL $AUTOMAKE $AUTOCONF $AUTORECONF $LIBTOOLIZE  $MAKE $SED; do
	if test x"$cmd" = x"none" ; then
		echo Required commands missing. Please check that your path is correct.
		exit 1
	fi
done

AC_SUBST([CC])
AC_SUBST([CXX])

AC_MSG_CHECKING([the gsbuild runtime path])
ac_rtpath=""
for pathcomponent in `
	(dirname $AUTOHEADER &&
	dirname $ACLOCAL &&
	dirname $AUTOMAKE &&
	dirname $AUTOCONF &&
	dirname $MAKE &&
	dirname $LD) | sort -r | uniq` ; do
	ac_rtpath=$pathcomponent:$ac_rtpath
done
AC_MSG_RESULT([$ac_rtpath])
AC_SUBST([ac_rtpath])

dnl Initialize maintainer mode
AM_MAINTAINER_MODE

dnl Checks for header files.

dnl Checks for typedefs, structures, and compiler characteristics.

AC_OUTPUT(Makefile compss_build)
