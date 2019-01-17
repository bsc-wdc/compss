#! /bin/sh
#
#   /**------- <| --------------------------------------------------------**
#    **         A                     Clan                                **
#    **---     /.\   -----------------------------------------------------**
#    **   <|  [""M#                 checker.sh                            **
#    **-   A   | #   -----------------------------------------------------**
#    **   /.\ [""M#         First version: 30/04/2008                     **
#    **- [""M# | #  U"U#U  -----------------------------------------------**
#         | #  | #  \ .:/
#         | #  | #___| #
# ******  | "--'     .-"  *****************************************************
# *     |"-"-"-"-"-#-#-##   Clan : the Chunky Loop Analyser (experimental)    *
# ****  |     # ## ######  ****************************************************
# *      \       .::::'/                                                      *
# *       \      ::::'/     Copyright (C) 2008 Cedric Bastoul                 *
# *     :8a|    # # ##                                                        *
# *     ::88a      ###      This is free software; you can redistribute it    *
# *    ::::888a  8a ##::.   and/or modify it under the terms of the GNU       *
# *  ::::::::888a88a[]:::   Lesser General Public License as published by     *
# *::8:::::::::SUNDOGa8a::. the Free Software Foundation, either version 3 of *
# *::::::::8::::888:Y8888::                the License, or (at your option)   *
# *::::':::88::::888::Y88a::::::::::::...  any later version.                 *
# *::'::..    .   .....   ..   ...  .                                         *
# * This software is distributed in the hope that it will be useful, but      *
# * WITHOUT ANY WARRANTY; without even the implied warranty of                *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General *
# * Public License  for more details.	                                      *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public          *
# * License along with software; if not, write to the Free Software           *
# * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA  *
# *                                                                           *
# * Clan, the Chunky Loop Analyser                                            *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/

output=0
nb_tests=0;

TEST_NAME="$1"    ## Name of the group of files to test

TEST_FILES="$2"   ## List of test file prefixes and individual options

TEST_OPTION="$3"  ## Option for clan

TEST_TYPE="$4"    ## - "clan" to simply test clan (default)
                  ## - "valgrind" to test the valgrind output

# Uncomment the following line to print the test script
#set -x verbose  #echo on

clan=$top_builddir/clan$EXEEXT

echo "[TEST] $TEST_NAME";

for i in $TEST_FILES; do

  nb_tests=$(($nb_tests + 1))
  outtemp=0
  # Since -autoinsert modifies the input file, we use a random temporary file
  # before calling clan.
  input="/tmp/$$-clan-input.c";
  clanout="/tmp/$$-clan-stderr"
  cp $i $input;

  if [ "$TEST_TYPE" = "clan" ]; then
    # Test the basic .scop generation
    filename=$(basename "$i");
    echo "[CHECK] Source parser test:== $i ==";
    $clan $TEST_OPTION $input > $input.scop 2>"$clanout"
    if [ "$TEST_OPTION" = "-autoinsert" ]; then
      diff --ignore-matching-lines="$filename" \
           --ignore-matching-lines="$input" \
           --ignore-matching-lines='enerated' \
           $input $i.orig;
      z=$?;
    else
      diff --ignore-matching-lines="$filename" \
           --ignore-matching-lines="$input" \
           --ignore-matching-lines='enerated' \
           $input.scop $i.scop;
      z=$?;
    fi
    err=`cat "$clanout"`;
    if [ "$z" -ne "0" ]; then
      echo "\033[31m[FAIL] Source parser test: Wrong output\n$z\033[0m";
      outtemp=1;
      output=1
    fi
    if ! [ -z "$err" ]; then
      echo "\033[33m[INFO] Source parser test stderr output:\n$err\033[0m";
    fi
    if [ $outtemp = "0" ]; then
      echo "[PASS] Source parser test: output OK";
    fi
  else
    echo "[VALCHECK] Source parser test:== $i ==";
    libtool --mode=execute valgrind --error-exitcode=1 $clan $TEST_OPTIONS $input > /dev/null 2> "$clanout";
    errors=$?;
    leaks=`grep "in use at exit" "$clanout" | cut -f 2 -d ':'`
    if [ "$errors" = "1" ]; then
      echo "\033[31mMemory error detected... \033[0m";
      cat "$clanout";
      output="1";
    elif [ "$leaks" != " 0 bytes in 0 blocks" ]; then
      echo "\033[31mMemory leak detected... \033[0m";
      cat "$clanout";
      output="1";
    else
      output="0";
    fi;
  fi
  rm -f "$input" "$input.scop" "$clanout"
done
if [ $output = "1" ]; then
  echo "\033[31m[FAIL] $1\033[0m";
else
  echo "[PASS] $1 ($nb_tests tests)";
fi
exit $output
