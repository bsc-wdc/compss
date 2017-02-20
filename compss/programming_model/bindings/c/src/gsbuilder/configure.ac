
dnl Process this file with autoconf to produce a configure script.
AC_INIT()

AM_INIT_AUTOMAKE(compsuperscalar, 0.0.0)
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
AC_PATH_PROG([SSH], [ssh], [none])
AC_PATH_PROG([BASH], [bash], [none])
AC_PATH_PROG([JAVA], [java], [none])
AC_PATH_PROG([RM], [rm], [none])
AC_PATH_PROG([MKDIR], [mkdir], [none])
AC_PATH_PROG([MV], [mv], [none])
AC_PATH_PROG([TOUCH], [touch], [none])
AC_PATH_PROG([SED], [sed], [none])

#AX_BOOST_BASE(1.54)
#AX_BOOST_SERIALIZATION

#AC_CONFIG_LINKS([test.txt:test.txt])

for cmd in $CC $LD $CHMOD $CAT $RM $CP $SSH $BASH $MKDIR $MV $TOUCH $AUTOHEADER $ACLOCAL $AUTOMAKE $AUTOCONF $AUTORECONF $LIBTOOLIZE  $MAKE $SED; do
	if test x"$cmd" = x"none" ; then
		echo Required commands missing. Please check that your path is correct.
		exit 1
	fi
done

AC_MSG_CHECKING([the execution environment])
AC_ARG_WITH([execution-environment],
        [  --with-execution-environment=[GRID|CLUSTER|MN_SCRATCH]  Set CLUSTER if the environment is a cluster with shared filesystem. Set MN_SCRATCH if the environment is MareNostrum using scratch disk.  If unsure set it to GRID.],
        [               
                if ! test x"$withval" == x"GRID" && ! test x"$withval" == x"CLUSTER" && ! test x"$withval" == x"MN_SCRATCH"; then
                        AC_MSG_RESULT([failed]) 
                        AC_MSG_ERROR([--with-execution-environment accepts GRID ,CLUSTER or MN_SCRATCH value])
                fi      
                AC_MSG_RESULT([${withval}])
                exec_environment=$withval
        ],
        [
                AC_MSG_RESULT([taking default value, GRID])
                exec_environment=GRID
        ]       
)               

copy_command="";
copy_command_flags="";
if test x"$exec_environment" == x"GRID"; then
	copy_command="@SCP@";
	copy_command_flags="-q -o StrictHostKeyChecking=no";
else
	if test x"$exec_environment" == x"MN_SCRATCH";then
		copy_command="@SCP@";
        	copy_command_flags="-q -o StrictHostKeyChecking=no";
	else
		copy_command="@CP@";
		copy_command_flags="-f";
	fi
fi
if test x"$exec_environment" == x"MN_SCRATCH"; then
	AC_PATH_PROG([SRUN], [srun], [none])
	AC_PATH_PROG([SL_MACHINE_LIST], [sl_get_machine_list], [none])
	AC_PATH_PROG([LL_MACHINE_LIST], [ll_get_machine_list], [none])
	for cmd in $SRUN $SL_MACHINE_LIST $LL_MACHINE_LIST; do
        if test x"$cmd" = x"none" ; then
                echo Required commands missing. Please check that your path is correct.
                AC_MSG_WARN([ queue submission program missing. Only interactive could run ]) 
        fi
done

fi
AC_SUBST([copy_command])
AC_SUBST([copy_command_flags])
AC_SUBST([exec_environment])
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
dnl Checks for queue submission command
QUEUE_SUBMIT_PBS=''
QUEUE_SUBMIT_LL=''
QUEUE_SUBMIT_MN=''
AC_PATH_PROG(QUEUE_SUBMIT_PBS, qsub, none)
AC_PATH_PROG(QUEUE_SUBMIT_LL, llsubmit, none)
AC_PATH_PROG(QUEUE_SUBMIT_MN, mnsubmit, none)
test x"$QUEUE_SUBMIT_PBS" = x"none" && test x"$QUEUE_SUBMIT_LL" = x"none" && test x"$QUEUE_SUBMIT_MN" = x"none" && AC_MSG_WARN([ queue job submission program missing ])

dnl Initialize maintainer mode
AM_MAINTAINER_MODE

dnl Checks for header files.

dnl Checks for typedefs, structures, and compiler characteristics.

AC_OUTPUT(Makefile gsbuild etc/Makefile)
