#!/bin/bash -e

  # Library build artefacts
  rm -f src/libbindings_common.la
  rm -f src/*.lo
  rm -f src/*.o

  rm -rf src/.deps
  rm -rf src/.libs

  rm -f src/Makefile
  rm -f src/Makefile.in
  rm -f Makefile.in

  # Test build artefacts (object files, generated test binary, libtool debris,
  # gcov coverage data, autotools test driver logs)
  rm -f tests/*.o tests/*.lo tests/*.la
  rm -f tests/*.gcno tests/*.gcda
  rm -f tests/*.log tests/*.trs
  rm -rf tests/.deps tests/.libs
  rm -f tests/test_gs_compss
  rm -f tests/Makefile tests/Makefile.in

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
