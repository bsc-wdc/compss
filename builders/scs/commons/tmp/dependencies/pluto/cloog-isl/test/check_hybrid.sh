#!/bin/sh
#
#   /**-------------------------------------------------------------------**
#    **                              CLooG                                **
#    **-------------------------------------------------------------------**
#    **                           check_hybrid.sh                         **
#    **-------------------------------------------------------------------**
#    **                 First version: May 11th 2015                      **
#    **-------------------------------------------------------------------**/
#

#/*****************************************************************************
# *               CLooG : the Chunky Loop Generator (experimental)            *
# *****************************************************************************
# *                                                                           *
# * Copyright (C) 2003 Cedric Bastoul                                         *
# *                                                                           *
# * This library is free software; you can redistribute it and/or             *
# * modify it under the terms of the GNU Lesser General Public                *
# * License as published by the Free Software Foundation; either              *
# * version 2.1 of the License, or (at your option) any later version.        *
# *                                                                           *
# * This library is distributed in the hope that it will be useful,           *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of            *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU         *
# * Lesser General Public License for more details.                           *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public          *
# * License along with this library; if not, write to the Free Software       *
# * Foundation, Inc., 51 Franklin Street, Fifth Floor,                        *
# * Boston, MA  02110-1301  USA                                               *
# *                                                                           *
# * CLooG, the Chunky Loop Generator                                          *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/

# Refactor $SPECIAL_OPTIONS list to remove quotes and to replace spaces in
# individual tests with %, e.g., "'file1 -f -1' 'file2'" becomes
# "file1%-f%-1 file2".
special_refactored=`echo "$SPECIAL_OPTIONS" | \
                    sed "s/'  *'/#/g"       | \
                    sed 's/ /%/g'           | \
                    sed "s/#/ /g"           | \
                    sed "s/'//g"`

$CHECKER "HYBRID C" "$CLOOGTEST_C" "" "cloog" "c" "hybrid"
failedtest=$?;

$CHECKER "HYBRID SPECIAL" "$special_refactored" "" "cloog" "c" "hybrid"
failedtest=`expr $failedtest + $?`;

$CHECKER "HYBRID SPECIAL -sh 1" "$special_refactored" "-sh 1" "cloog" "c" "hybrid"
failedtest=`expr $failedtest + $?`;

$CHECKER "HYBRID STRIDED" "$CLOOGTEST_STRIDED" "-strides 1" "cloog" "c" "hybrid"
failedtest=`expr $failedtest + $?`;

if ! [ -z $CLOOGTEST_OPENSCOP ] ; then
$CHECKER "HYBRID OPENSCOP" "$CLOOGTEST_OPENSCOP" "-openscop" "scop" "c" "hybrid"
failedtest=`expr $failedtest + $?`;
fi

exit $failedtest;
