#!/bin/sh

#
#   /**-------------------------------------------------------------------**
#    **                              CAnDL                                **
#    **-------------------------------------------------------------------**
#    **                           Makefile.am                             **
#    **-------------------------------------------------------------------**
#    **                 First version: june 29th 2012                     **
#    **-------------------------------------------------------------------**/
#
#/*****************************************************************************
# *   CAnDL : the Chunky Analyser for Dependences in Loops (experimental)     *
# *****************************************************************************
# *                                                                           *
# * Copyright (C) 2003-2008 Cedric Bastoul                                    *
# *                                                                           *
# * This is free software; you can redistribute it and/or modify it under the *
# * terms of the GNU Lesser General Public License as published by the Free   *
# * Software Foundation; either version 3 of the License, or (at your option) *
# * any later version.							      *
# *                                                                           *
# * This software is distributed in the hope that it will be useful, but      *
# * WITHOUT ANY WARRANTY; without even the implied warranty of                *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General *
# * Public License for more details.                                          *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public License  *
# * along with software; if not, write to the Free Software Foundation, Inc., *
# * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA                    *
# *                                                                           *
# * CAnDL, the Chunky Dependence Analyser                                     *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/

TEST_NAME="$1"    ## Name of the group of files to test

TEST_FILES="$2"   ## List of test file prefixes and individual options

TEST_TRANSFO="$3" ## Booelan set to 1 if the test is about checking a
                  ## transformation, 0 if it is a dependence analysis

TEST_TYPE="$4"    ## - "candl" to simply test candl (default)
                  ## - "valgrind" to test the valgrind output

# Uncomment the following line to print the test script
#set -x verbose  #echo on

candl=$top_builddir/candl$EXEEXT
failed=0

echo "             /*-----------------------------------------------*"
echo "              *     Testing Candl: $TEST_NAME"
echo "              *-----------------------------------------------*/"

for name in $TEST_FILES; do

  orig_scop="$name.c.orig.scop"
  struct="$name.c.struct"
  clay_scop="$name.c.clay.scop"  # only for transformations tests

  # read candl options
  options=`grep "candl options" "$name.c" | cut -d'|' -f2`
  
  if [ "$TEST_TYPE" = "candl" ]; then
    echo "check $name \c"
    case $TEST_TRANSFO in
      0)
        $candl $options "$orig_scop" -struct | grep -v "enerated by" > candl_temp
        ;;
      1)
        $candl $options "$clay_scop" -test "$orig_scop" -struct | grep -v "enerated by" > candl_temp
        ;;
    esac
  
    result=`diff candl_temp "$struct" | wc -l`
  else
    echo "valcheck $name \c"
    case $TEST_TRANSFO in
      0)
        libtool --mode=execute valgrind --error-exitcode=1 \
                $candl $options "$orig_scop" -struct > /dev/null 2> candl_temp;
        ;;
      1)
        libtool --mode=execute valgrind --error-exitcode=1 \
                $candl $options "$clay_scop" -test "$orig_scop" -struct > /dev/null 2> candl_temp;
        ;;
    esac
  
    errors=$?;
    leaks=`grep "in use at exit" candl_temp | cut -f 2 -d ':'`
    if [ "$errors" = "1" ]; then
      echo -e "\033[31mMemory error detected... \033[0m";
      cat candl_temp;
      result="1";
    elif [ "$leaks" != " 0 bytes in 0 blocks" ]; then
      echo -e "\033[31mMemory leak detected... \033[0m";
      cat candl_temp;
      result="1";
    else
      result="0";
    fi;
  fi

  if [ $result -ne 0 ]; then
    echo "\033[31m[ FAIL ]\033[0m"
    failed=1
  else
    echo "\033[32m[ OK ]\033[0m"
  fi

  rm -f candl_temp
done
exit $failed
