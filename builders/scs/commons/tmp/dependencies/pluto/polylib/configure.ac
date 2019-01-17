dnl	This file is an input file used by the GNU "autoconf" program to
dnl	generate the file "configure", which is run to configure the
dnl	Makefile in this directory.

#    This file is part of PolyLib.                                                         
#
#    PolyLib is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or   
#    (at your option) any later version.                                 
#
#    PolyLib is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
#    GNU General Public License for more details.                  
#
#    You should have received a copy of the GNU General Public License
#    along with PolyLib.  If not, see <http://www.gnu.org/licenses/>.

# Authors: Bart Kienhuis, Vincent Loechner, T. Risset
# Copyright (c) 2000 The Regents of the University of California.
# All rights reserved.
# 
# Permission is hereby granted, without written agreement and without
# license or royalty fees, to use, copy, modify, and distribute this
# software and its documentation for any purpose, provided that the
# above copyright notice and the following two paragraphs appear in all
# copies of this software.
# 
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY
# FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
# ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
# THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
# 
# THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE
# PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF
# CALIFORNIA HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
# ENHANCEMENTS, OR MODIFICATIONS.
# 
# 						PT_COPYRIGHT_VERSION_2
# 						COPYRIGHTENDKEY

dnl Process this file with autoconf to produce a configure script.

AC_INIT(polylib, 5.22.5b)
AC_CONFIG_AUX_DIR([.])
AM_INIT_AUTOMAKE([gnu])
AC_CONFIG_SRCDIR(include/polylib/polylib.h.in)
AC_CONFIG_MACRO_DIR([m4])

AC_PREREQ(2.60)

AC_SUBST(versioninfo)
versioninfo=9:0:1

if test "x$prefix" != "xNONE"; then
    prefix_wd=`cd $prefix && pwd`
    srcdir_wd=`cd $srcdir && pwd`
    wd=`pwd`
    if test "x$prefix_wd" = "x$srcdir_wd"; then
	AC_MSG_ERROR(Installation in source directory not supported)
    fi
    if test "x$prefix_wd" = "x$wd"; then
	AC_MSG_ERROR(Installation in build directory not supported)
    fi
fi

dnl Checks for programs.
AC_LANG(C)
AC_PROG_CC
AC_PROG_LN_S
AC_PROG_INSTALL
AC_PROG_LIBTOOL
AM_PROG_CC_C_O

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS(limits.h unistd.h)
AC_CHECK_HEADERS(getopt.h)


dnl /*****************************************************************************/
dnl See if the only goal is to create an 'int', 'longint', or 'longlongint' library

AC_CHECK_SIZEOF(int,1)
AC_CHECK_SIZEOF(long int,1)
AC_CHECK_SIZEOF(long long int,1)

AC_SUBST(polylib32_defs)
AC_SUBST(polylib64_defs)
AC_SUBST(polylib128_defs)

for type in long_long_int long_int int; do
    case "$type" in
	int)
	    bits=`expr $ac_cv_sizeof_int \* 8`
	    defs="#define LINEAR_VALUE_IS_INT"
	    ;;
	long_int)
	    bits=`expr $ac_cv_sizeof_long_int \* 8`
	    defs="#define LINEAR_VALUE_IS_LONG
#define LINEAR_VALUE_PROTECT_MULTIPLY"
	    ;;
	long_long_int)
	    bits=`expr $ac_cv_sizeof_long_long_int \* 8`
	    defs="#define LINEAR_VALUE_IS_LONGLONG
#define LINEAR_VALUE_PROTECT_MULTIPLY
#define LINEAR_VALUE_ASSUME_SOFTWARE_IDIV"
	    ;;
    esac
    case "$bits" in
	32)
	    polylib32_defs=$defs
	    ;;
	64)
	    polylib64_defs=$defs
	    ;;
	128)
	    polylib128_defs=$defs
	    ;;
    esac
done


AC_SUBST(polylibs)
AC_SUBST(ALL_BITS)

AC_ARG_ENABLE(int-lib,
	[  --enable-int-lib        Build an int library], 
        [lib_type=int
	 bits=`expr $ac_cv_sizeof_int \* 8`
	 polylibs="libpolylib$bits.la $polylibs"
	 ALL_BITS=" $bits $ALL_BITS"
	])
AC_ARG_ENABLE(longint-lib,
	[  --enable-longint-lib    Build a long int library], 
        [lib_type=longint
	 bits=`expr $ac_cv_sizeof_long_int \* 8`
	 case "$ALL_BITS" in
	   *' '$bits' '*) ;;
	   *) polylibs="libpolylib$bits.la $polylibs"
	      ALL_BITS=" $bits $ALL_BITS" ;;
	 esac
	])
AC_ARG_ENABLE(longlongint-lib,
	[  --enable-longlongint-lib Build a long long int library], 
        [lib_type=longlongint
	 bits=`expr $ac_cv_sizeof_long_long_int \* 8`
	 case "$ALL_BITS" in
	   *' '$bits' '*) ;;
	   *) polylibs="libpolylib$bits.la $polylibs"
	      ALL_BITS=" $bits $ALL_BITS" ;;
	 esac
	])

dnl Check for GMP library
AC_MSG_CHECKING(whether to use GMP)
AC_ARG_WITH(libgmp, 
        [  --with-libgmp[=DIR]     Build a GMP polylib using the GMP library], 
	gmp_package=$withval, gmp_package=notuserdefined)
if test "x$gmp_package" = "xno"; then
	# user choice: don't use gmp
	AC_MSG_RESULT([no])
elif test "x$gmp_package" = "xnotuserdefined"; then
	# check if we find gmp or not... Don't fail if not
	AC_MSG_RESULT(system)
	AC_CHECK_HEADER(gmp.h,
		[AC_CHECK_LIB(gmp,main,
		    [poly_cv_gmpfatal="no"],
		    [poly_cv_gmpfatal="yes"])],
		[poly_cv_gmpfatal="yes"])
	if test "$poly_cv_gmpfatal" = "yes"; then
		AC_MSG_RESULT([GMP not found])
		gmp_package=no
	else
		AC_CHECK_DECLS(mp_get_memory_functions,[],[
			AC_LIBOBJ(mp_get_memory_functions)
		],[#include <gmp.h>])
	fi
else
	# user choice: use gmp
	AC_MSG_RESULT(yes)
	if test "x$gmp_package" != "xyes"; then
	    GMP_DIR=$gmp_package
	    if test ! -d "$GMP_DIR"; then
	    	AC_ERROR(Directory given for GMP Distribution is not a directory)
	    fi
	    CPPFLAGS="-I$GMP_DIR/include $CPPFLAGS"
	    LDFLAGS="-L$GMP_DIR/lib $LDFLAGS"
	fi
	AC_CHECK_HEADER(gmp.h,
		[AC_CHECK_LIB(gmp,main,
		    [poly_cv_gmpfatal="no"],
		    [poly_cv_gmpfatal="yes"])],
		[poly_cv_gmpfatal="yes"])
	if test "$poly_cv_gmpfatal" = "yes"; then
		AC_MSG_ERROR([GMP not found])
	else
		AC_CHECK_DECLS(mp_get_memory_functions,[],[
			AC_LIBOBJ(mp_get_memory_functions)
		],[#include <gmp.h>])
	fi

	lib_type=gmp
	BITS=`expr gmp`
	LIBS="-lgmp $LIBS"
	polylibs="libpolylib$BITS.la $polylibs"
	ALL_BITS="$BITS $ALL_BITS"
fi

# no default libtype defined. Let's try to build all of them.
if test "X$lib_type" = "X"; then
	AC_MSG_RESULT(no default type given by user:)
	bits=`expr $ac_cv_sizeof_int \* 8`
	AC_MSG_RESULT([ Adding 'int' ($bits) representation])
	lib_type=int
	polylibs="libpolylib$bits.la $polylibs"
	ALL_BITS="$bits $ALL_BITS"

	if test "$ac_cv_sizeof_long_int" -gt "$ac_cv_sizeof_int"; then
		bits=`expr $ac_cv_sizeof_long_int \* 8`
		AC_MSG_RESULT([Adding 'long int' ($bits)representation])
		lib_type=longint
		polylibs="libpolylib$bits.la $polylibs"
		ALL_BITS="$ALL_BITS $bits"
	fi
	if test "$ac_cv_sizeof_long_long_int" -gt "$ac_cv_sizeof_long_int"; then
		bits=`expr $ac_cv_sizeof_long_long_int \* 8`
		AC_MSG_RESULT([ Adding 'long long int' ($bits) representation])
		lib_type=longlongint
		polylibs="libpolylib$bits.la $polylibs"
		ALL_BITS="$ALL_BITS $bits"
	fi
	if test "x$gmp_package" != "xno"; then
		AC_MSG_RESULT([ Adding 'gmp' representation])
		lib_type=gmp
		BITS=`expr gmp`
		LIBS="-lgmp $LIBS"
		polylibs="libpolylib$BITS.la $polylibs"
		ALL_BITS="$ALL_BITS $BITS"
	fi
fi

case "$lib_type" in
	int)
	    BITS=`expr $ac_cv_sizeof_int \* 8`
	    ;;
	longint)
	    BITS=`expr $ac_cv_sizeof_long_int \* 8`
	    ;;
	longlongint)
	    BITS=`expr $ac_cv_sizeof_long_long_int \* 8`
	    ;;
esac


AC_SUBST(polylib)
polylib="libpolylib$BITS.la"

dnl extra indirection to avoid automake getting confused
AC_SUBST(multi_bin_programs)
AC_SUBST(multi_noinst_programs)
for pl_bit in $ALL_BITS ; do
	multi_bin_programs="$multi_bin_programs \$(multi_bin_programs_${pl_bit})"
	multi_noinst_programs="$multi_noinst_programs \$(no_inst_programs_${pl_bit})"
done

dnl /*****************************************************************************/
AC_ARG_ENABLE(extra-suffix,
	[  --enable-extra-suffix   Add bits size suffix to executables],
	[ if test "$enableval" = yes; then
		program_transform_name="s/\$\$/$BITS/;$program_transform_name"
    fi ] )

dnl /********************************************************************/
dnl Check for DOXYGEN package
AC_ARG_WITH(doxygen, [  --with-doxygen          DIR Location of the Doxygen Distribution (http://www.doxygen.org)], doxygen_package=$withval)

dnl AC_MSG_CHECKING(for Doxygen program)
AC_CHECK_PROG(DOXYGEN,doxygen,doxygen,test,[$PATH:$doxygen_package/bin:/usr/local/bin])
dnl AC_MSG_RESULT($DOXYGEN)


dnl AC_CHECK_LIB(pthread, pthread_create)
ACX_PTHREAD()
AC_ARG_ENABLE(threadsafe,
	[  --enable-threadsafe     threadsafe polylib (enabled by default if a threading library is found)],
	[
		if test "$enable_threadsafe" = "yes" -a "X$PTHREAD_CC" = "X"; then
			AC_MSG_ERROR([pthread library not found])
		fi
	] )

if test "X$PTHREAD_CC" = "X" ; then
	AC_MSG_WARN([No pthread library found, PolyLib will not be threadsafe])
elif test "X$PTHREAD_CC" != "X" ; then
	LIBS="$LIBS $PTHREAD_LIBS"
	CFLAGS="$CFLAGS $PTHREAD_CFLAGS -DTHREAD_SAFE_POLYLIB"
	CC="$PTHREAD_CC"
	AC_MSG_NOTICE([Will build a threadsafe library])
fi

AC_SUBST(LIBS)
AC_SUBST(CFLAGS)
AC_SUBST(CC)

dnl long/int bits and compilation flags
AC_SUBST(BITS)

dnl Used with doxygen
AC_SUBST(DOXYGEN)

dnl Currently, we only create one .pc file
dnl Creating one for each configured version is a bit complicated
PACKAGE_NAME=polylib$BITS
AX_CREATE_PKGCONFIG_INFO

AC_CONFIG_HEADERS(config.h)
AC_OUTPUT(Makefile
	  include/Makefile
	  include/polylib/Makefile
	  include/polylib/polylib.h
	  Test/Makefile
	  Test/Zpolytest/Makefile
	  Test/ehrhart/Makefile
	  Test/ehrhart_union/Makefile
	  Test/ranking/Makefile
	  Test/general/Makefile
	  Test/pp/Makefile
	  polylib.doxygen
	)


echo "---------------------------------------------------"
echo "Polylib will be built with" $ALL_BITS "integer size(s)"
echo "You can proceed with:"
echo "make               :to build the libs and the executables"
echo "make tests         :to test the library"
echo "make install       :to install them (to $prefix)"
echo "---------------------------------------------------"
