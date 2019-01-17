#
#   /**-------------------------------------------------------------------**
#    **                              CAnDL                                **
#    **-------------------------------------------------------------------**
#    **                           Makefile.am                             **
#    **-------------------------------------------------------------------**
#    **                 First version: june 28th 2012                     **
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

# author Joel Poudroux

# Will regenerate all the .scop and .depcandl of each test
# If a scop was already generated, the file is unchanged
# You can give the -a option to force to regenerate all the files


find -name *.c | grep 'unitary\|transformations' | while read name
do
  orig_scop="$name.orig.scop"
  struct="$name.struct"
  clay_scop="$name.clay.scop"  # only for transformations tests

  if [ ! -f "$orig_scop" ] || [ "$1" = "-a" ]; then

    rm -f "$orig_scop"
    rm -f "$struct"
    rm -f "$clay_scop"

    echo "add $name"

    # read candl options
    candloptions=`grep "candl options" "$name" | cut -d'|' -f2`

    clan -castle 0 "$name" | grep -v "enerated by" >"$orig_scop"

    # type of test
    type=`echo "$name" | cut -d/ -f2`
    case $type in
       "unitary")
          candl "$orig_scop" $candloptions -struct | grep -v "enerated by" >"$struct"
        ;;

      "transformations")
          rm -f "$clay_scop"
          clay "$orig_scop" | grep -v "enerated by">"$clay_scop"
          candl "$clay_scop" $candloptions -test "$orig_scop" -struct | \
              grep -v "enerated by" >"$struct"
        ;;
    esac
  fi

done

