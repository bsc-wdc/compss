#/bin/bash -e

  rm -rf src/libbindings_common.la
  rm -rf src/libbindings_common_la-GS_compss.lo
  rm -rf src/libbindings_common_la-GS_compss.o
  rm -rf src/.deps
  rm -rf src/.libs
  rm -rf src/Makefile
  rm -rf src/Makefile.in
  rm -rf m4
  mkdir -p m4
  rm -rf aclocal.m4
  rm -rf autom4te.cache
  rm -rf configure
  rm -rf INSTALL
  rm -rf Makefile.in
  rm -rf config
  rm -rf config.status
  rm -rf Makefile
  rm -rf libtool
  rm -rf config.log

