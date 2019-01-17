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

$CHECKER "Unitary Tests" "$UNITARY_TEST_FILES" 0 "${1:-candl}"
