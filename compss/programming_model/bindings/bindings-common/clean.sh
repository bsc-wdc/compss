#!/bin/bash -e

  rm -f src/libbindings_common.la
  rm -f src/libbindings_common_la-GS_compss.lo
  rm -f src/libbindings_common_la-GS_compss.o

  rm -rf src/.deps
  rm -rf src/.libs

  rm -f src/Makefile
  rm -f src/Makefile.in
  rm -f Makefile.in

  rm -rf m4
  rm -f aclocal.m4
  rm -rf autom4te.cache

  rm -f configure
  rm -rf config
  rm -f config.status
  rm -f config.log
  rm -f INSTALL
  rm -f libtool

  mkdir -p m4
