
dnl /**-------------------------------------------------------------------**
dnl  **                              PIPLIB                               **
dnl  **-------------------------------------------------------------------**
dnl  **                           configure.in                            **
dnl  **-------------------------------------------------------------------**
dnl  **                   First version: august 11th  2001                **
dnl  **-------------------------------------------------------------------**/
dnl
dnl Input file for autoconf to build a configuration shellscript.
dnl To build the configure script from the PipLib's top-level directory, use
dnl 	autoconf -I autoconf -o configure autoconf/configure.in
dnl for older versions of autoconf use -l instead
dnl 	autoconf -l autoconf autoconf/configure.in > configure
dnl 	chmod +x configure

m4_define([version_major], [1])
m4_define([version_minor], [4])
m4_define([version_revision], [0])

AC_PREREQ(2.53)
AC_INIT([piplib], [version_major.version_minor.version_revision],
        [piplib-development@googlegroups.com])
AC_CONFIG_AUX_DIR(autoconf)
AC_CONFIG_SRCDIR([include/piplib/piplib.h])
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([foreign])
m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])

AC_SUBST(versioninfo)
versioninfo=3:0:1


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

AC_CHECK_PROG(DOXYGEN, doxygen, doxygen, [])
AC_CHECK_PROG(TEXI2PDF, texi2pdf, texi2pdf, [])
AM_CONDITIONAL(HAVE_TEXI2DVI, test -n "$TEXI2PDF" -a -n "$DOXYGEN")

AX_CC_MAXOPT

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([errno.h stddef.h stdlib.h string.h strings.h unistd.h])

dnl Checks sizeof the two supported cases.
AC_CHECK_SIZEOF(int,1)
AC_CHECK_SIZEOF(long long int,1)


dnl /**************************************************************************
dnl  *                             Option setting                             *
dnl  **************************************************************************/


dnl The different packages that can be built and installed.
PIP="pip"
SP_DFLAGS=-DPIPLIB_INT_SP
DP_DFLAGS=-DPIPLIB_INT_DP
MP_DFLAGS=-DPIPLIB_INT_GMP
NEED_MP="no"
OSL_DFLAGS=-DPIPLIB_INT_OSL
NEED_OSL="no"


dnl Options.
dnl --with-pip=yes, --with-pip=no or --without-pip
AC_ARG_WITH(pip,
	[  --without-pip           disable the compilation of pip],
	[ echo "Package pip : $withval" &&
	  if test "$withval"="no" ; then 	
		PIP=""
	  fi ])

AC_ARG_WITH(gmp,
	    [AS_HELP_STRING([--with-gmp],
			    [Build gmp version])])
if test "x$with_gmp" = "x"; then
	with_gmp=yes
fi
AC_ARG_WITH(gmp_prefix,
	    [AS_HELP_STRING([--with-gmp-prefix=DIR],
			    [Prefix of gmp installation])])
AC_ARG_WITH(gmp_exec_prefix,
	    [AS_HELP_STRING([--with-gmp-exec-prefix=DIR],
			    [Exec prefix of gmp installation])])

if test "x$with_gmp_prefix" != "x" -o "x$with_gmp_exec_prefix" != "x"; then
	with_gmp=yes
fi

if test "x$with_gmp" = "xyes"; then
	NEED_MP="yes"
fi

if test "x$with_gmp_prefix" != "x" -a "x$with_gmp_exec_prefix" = "x"; then
	with_gmp_exec_prefix=$with_gmp_prefix
fi

AC_ARG_WITH(bits,
	[AS_HELP_STRING([--with-bits=32,64,gmp,all],
			[integer type version(s) to build (default is all)])])

TEST_MP="yes"
TEST_OSL="yes"
TO_BUILD="_sp _dp"

case "$with_bits" in
*all*|"")
	;;
*)
	TO_BUILD=""
	TEST_MP="no"
	TEST_OSL="no"
	case "$with_bits" in *32*) TO_BUILD="$TO_BUILD _sp" ;; esac
	case "$with_bits" in *64*) TO_BUILD="$TO_BUILD _dp" ;; esac
	case "$with_bits" in *MP*) TEST_MP="yes"; NEED_MP="yes" ;; esac
	case "$with_bits" in *OSL*) TEST_OSL="yes"; NEED_OSL="yes" ;; esac
	;;
esac


dnl /**************************************************************************
dnl  *                            Where is GMP?                               *
dnl  **************************************************************************/


dnl Checking for gmp
if test "$with_gmp" = "no"; then
  echo "GMP package not defined"
else
  if test "$TEST_MP" != "no"; then
    if test "x$with_gmp_prefix" != "x" ; then
      CPPFLAGS="-I$with_gmp_prefix/include $CPPFLAGS"
    fi
    
    if test "x$with_gmp_exec_prefix" != "x" ; then
      EXTRA_LIBS="$EXTRA_LIBS -L$with_gmp_exec_prefix/lib"
      LDFLAGS="-L$with_gmp_exec_prefix/lib $LDFLAGS"
    fi
     
    gmp_found=no
    AC_CHECK_HEADER(gmp.h, [
	AC_CHECK_LIB(gmp, __gmpz_init,
		     [gmp_found=yes],
		     [AC_MSG_WARN(Can't find gmp library.)])
    ], [AC_MSG_WARN(Can't find gmp headers.)])
    if test $gmp_found = yes; then
	EXTRA_LIBS="$EXTRA_LIBS -lgmp"
	TO_BUILD="$TO_BUILD _gmp"
    else
	if test $NEED_MP = yes; then
		AC_MSG_ERROR([gmp not found])
	fi
    fi

    AC_MSG_CHECKING(whether gmp works)
    AC_MSG_RESULT($gmp_found)
  fi
fi


dnl /**************************************************************************
dnl  *                       Packages to actually build                       *
dnl  **************************************************************************/


for tb in $TO_BUILD; do
    piplibs="$piplibs libpiplib$tb.la"
    if test "x$PIP" != "x"; then
	pips="$pips pip$tb\$(EXEEXT)"
    fi
    examples="$examples example$tb\$(EXEEXT)"

    cmakeconfigs="$cmakeconfigs piplib$tb"
    case "$tb" in
    _sp) cmakeconfigs="$cmakeconfigs piplib32"
         ;;
    _dp) cmakeconfigs="$cmakeconfigs piplib64"
         ;;
    _gmp) cmakeconfigs="$cmakeconfigs piplibmp"
         ;;
    esac;
done

AC_SUBST(GIT_INDEX)
if test -f $srcdir/.git/HEAD; then
    GIT_INDEX="\$(top_srcdir)/.git/index"
fi

dnl /**************************************************************************
dnl  *                            Substitutions                               *
dnl  **************************************************************************/


dnl Substitutions to do in Makefile.in.
AC_SUBST(examples)
AC_SUBST(piplibs)
AC_SUBST(pips)
AC_SUBST(cmakeconfigs)
AC_SUBST(EXTRA_LIBS)

dnl compilation flags
AC_SUBST(SP_DFLAGS)
AC_SUBST(DP_DFLAGS)
AC_SUBST(MP_DFLAGS)

AC_CONFIG_FILES(Makefile example/Makefile test/Makefile doc/Makefile)
AC_CONFIG_FILES([genversion.sh], [chmod +x genversion.sh])
AC_CONFIG_COMMANDS([version.h],
    [echo '#define PIPLIB_HEAD "'`./genversion.sh`'"' > version.h])
AC_CONFIG_COMMANDS([doc/gitversion.texi],
    [echo '@set VERSION '`./genversion.sh`'' > doc/gitversion.texi])
AC_OUTPUT


echo "             /*-----------------------------------------------*"
echo "              *         PIP/PipLib configuration is OK        *"
echo "              *-----------------------------------------------*/"
echo "It appears that your system is OK to start PIP/PipLib compilation. You"
echo "need now to type \"make uninstall\" if your are upgrading an old version."
echo "Next, type \"make\" to build the program. Lastly type \"make install\" to"
echo "install it on your system (log as root if necessary)."
