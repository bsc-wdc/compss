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

dnl --- JNI/Java detection ---
AC_ARG_VAR([JAVA_HOME], [Path to Java home (JDK)])
AC_PATH_PROG([JAVA], [java])
if test -z "$JAVA_HOME" -a -n "$JAVA"; then
  JAVA_HOME=`$JAVA -XshowSettings:properties -version 2>&1 | sed -n 's/.*java.home = //p' | head -n1`
fi
if test -z "$JAVA_HOME"; then
  AC_MSG_ERROR([JAVA_HOME is not set and could not be detected. Please export JAVA_HOME pointing to your JDK.])
fi

case "$host_os" in
  linux*)  CPP_OS="-I$JAVA_HOME/include/linux" ;;
  darwin*) CPP_OS="-I$JAVA_HOME/include/darwin" ;;
  freebsd*) CPP_OS="-I$JAVA_HOME/include/freebsd" ;;
  *)       CPP_OS="" ;;
esac

JAVA_LIB_DIR=""
for d in \
  "$JAVA_HOME/lib/server" \
  "$JAVA_HOME/jre/lib/amd64/server" \
  "$JAVA_HOME/jre/lib/server" \
  "$JAVA_HOME/lib/amd64/server"; do
  if test -d "$d"; then
    JAVA_LIB_DIR="$d"
    break
  fi
done
if test -z "$JAVA_LIB_DIR"; then
  AC_MSG_WARN([Could not locate libjvm directory under JAVA_HOME; linking may fail.])
fi

AC_SUBST([JAVA_HOME])
AC_SUBST([CPP_OS])
AC_SUBST([JAVA_LIB_DIR])

AC_OUTPUT(Makefile src/Makefile tests/Makefile)
