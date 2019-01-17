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

############################################################################
SUBDIRS 		=

#############################################################################

MAINTAINERCLEANFILES    = Makefile.in

#############################################################################

UNITARY_TEST_FILES = \
	unitary/ax-do \
	unitary/can_reorder_loop \
	unitary/gemver \
	unitary/jacobi1d \
	unitary/loulou \
	unitary/lu \
	unitary/matmul \
	unitary/multiscop3 \
	unitary/no-unions \
	unitary/scalpriv \
	unitary/scalexp \
	unitary/simple \
	unitary/swim \
	unitary/union1 \
	unitary/union2 \
	unitary/union3

TRANSFO_FAIL_TEST_FILES = \
	transformations/must_fail/no_scalren \
	transformations/must_fail/union1 \
	transformations/must_fail/union2 \
	transformations/must_fail/union3

TRANSFO_WORKING_TEST_FILES = \
	transformations/working/can_reorder_loop \
	transformations/working/loulou \
	transformations/working/scalren \
	transformations/working/union

TESTS_ENVIRONMENT = \
	top_builddir=$(top_builddir) \
	srcdir=$(srcdir) \
	EXEEXT=$(EXEEXT) \
	CHECKER="$(top_srcdir)/tests/checker.sh" \
	COMPILE="$(COMPILE)" \
	LINK="$(LINK)" \
	UNITARY_TEST_FILES="$(UNITARY_TEST_FILES)" \
	TRANSFO_FAIL_TEST_FILES="$(TRANSFO_FAIL_TEST_FILES)" \
	TRANSFO_WORKING_TEST_FILES="$(TRANSFO_WORKING_TEST_FILES)"

valcheck:
	$(TESTS_ENVIRONMENT) $(srcdir)/check_unitary.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_working.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_fail.sh valgrind

check_SCRIPTS = \
	check_unitary.sh \
	check_fail.sh \
	check_working.sh

## Replace by program names when needed
# check_PROGRAMS = ...

TESTS = $(check_SCRIPTS)

EXTRA_DIST = \
	$(check_SCRIPTS) \
	$(srcdir)/checker.sh \
	$(UNITARY_TEST_FILES:%=%.c) \
	$(UNITARY_TEST_FILES:%=%.c.struct) \
	$(UNITARY_TEST_FILES:%=%.c.orig.scop) \
	$(TRANSFO_FAIL_TEST_FILES:%=%.c) \
	$(TRANSFO_FAIL_TEST_FILES:%=%.c.struct) \
	$(TRANSFO_FAIL_TEST_FILES:%=%.c.orig.scop) \
	$(TRANSFO_WORKING_TEST_FILES:%=%.c) \
	$(TRANSFO_WORKING_TEST_FILES:%=%.c.struct) \
	$(TRANSFO_WORKING_TEST_FILES:%=%.c.orig.scop)
