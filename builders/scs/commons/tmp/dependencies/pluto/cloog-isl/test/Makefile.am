#
#   /**-------------------------------------------------------------------**
#    **                              CLooG                                **
#    **-------------------------------------------------------------------**
#    **                            makefile                               **
#    **-------------------------------------------------------------------**
#    **                   First version: march 6th 2003                   **
#    **-------------------------------------------------------------------**/
#
# makefile for checking.

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
# * This file has been written with the help of the Bart Kienhuis's checking  *
# * makefile for PipLib.                                                      *
# *                                                                           *
# *****************************************************************************/

if NO_ISL
GENERATE_TEST_ADVANCED =
else
GENERATE_TEST_ADVANCED = generate_test_advanced
endif
noinst_PROGRAMS = $(GENERATE_TEST_ADVANCED)
AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
LDADD = ../libcloog-isl.la
generate_test_advanced_SOURCES = generate_test_advanced.c

FORCE:
../libcloog-isl.la: FORCE
	cd ..; $(MAKE) $(AM_MAKEFLAGS) libcloog-isl.la

ROOT = ..

FINITE_CLOOGTEST_C = \
	0D-1 \
	0D-2 \
	0D-3 \
	1point-1 \
	1point-2 \
	4-param \
	basic-bounds-1 \
	basic-bounds-2 \
	basic-bounds-3 \
	basic-bounds-4 \
	basic-bounds-5 \
	basic-bounds-6 \
	block \
	block2 \
	block3 \
	byu98-1-2-3 \
	cholesky \
	cholesky2 \
	christian \
	classen \
	classen2 \
	constant \
	constbound \
	darte \
	dealII \
	donotsimp \
	dot \
	dot2 \
	double \
	durbin_e_s \
	emploi \
	esced \
	ex1 \
	forwardsub-1-1-2 \
	forwardsub-2-1-2-3 \
	forwardsub-3-1-2 \
	gauss \
	gesced \
	gesced2 \
	gesced3 \
	guide \
	iftest \
	iftest2 \
	largeur \
	levenshtein-1-2-3 \
	lex \
	lineality-1-2 \
	lineality-2-1-2 \
	logo \
	logopar \
	lu \
	lu2 \
	lub \
	lux \
	min-1-1 \
	min-2-1 \
	min-3-1 \
	min-4-1 \
	mode \
	multi-mm-1 \
	no_lindep \
	orc \
	rectangle \
	singleton \
	square+triangle-1-1-2-3 \
	swim \
	test \
	thomasset \
	tiling \
	uday_scalars \
	union \
	vivien \
	vivien2 \
	walters \
	walters2 \
	walters3 \
	wavefront \
	yosr \
	yosr2 \
	non_optimal/nul_complex1 \
	non_optimal/usvd_e_t \
	non_optimal/youcef \
	reservoir/cholesky2 \
	reservoir/fusion1 \
	reservoir/fusion2 \
	reservoir/jacobi2 \
	reservoir/jacobi3 \
	reservoir/lim-lam1 \
	reservoir/lim-lam2 \
	reservoir/lim-lam3 \
	reservoir/lim-lam4 \
	reservoir/lim-lam5 \
	reservoir/lim-lam6 \
	reservoir/liu-zhuge1 \
	reservoir/loechner3 \
	reservoir/loechner4 \
	reservoir/loechner5 \
	reservoir/mg-interp2 \
	reservoir/mg-interp \
	reservoir/mg-psinv \
	reservoir/mg-resid \
	reservoir/mg-rprj3 \
	reservoir/pingali1 \
	reservoir/pingali2 \
	reservoir/pingali3 \
	reservoir/pingali4 \
	reservoir/pingali5 \
	reservoir/pingali6 \
	reservoir/QR \
	reservoir/tang-xue1 \
	reservoir/two

CLOOG_ISL_TEST_C = \
	isl/mod \
	isl/mod2 \
	isl/mod3 \
	isl/mod4

CLOOGTEST_C = \
	infinite \
	infinite2 \
	infinite3 \
	infinite4 \
	$(FINITE_CLOOGTEST_C) \
	$(CLOOG_ISL_TEST_C)

CLOOGTEST_FORTRAN = \
	dartef \
	iftestf \
	levenshtein-1-2-3f \
	yosrf

CLOOG_ISL_TEST_STRIDED = \
	isl/mxm-shared

CLOOGTEST_STRIDED = \
	basic-bounds-2 \
	mod4 \
	nul_basic1 \
	nul_basic2 \
	nul_lcpc \
	multi-stride \
	multi-stride2 \
	stride3 \
	stride4 \
	reservoir/bastoul3 \
	reservoir/stride \
	reservoir/stride2 \
	$(CLOOG_ISL_TEST_STRIDED)

if NO_OSL
CLOOGTEST_OPENSCOP =
else
CLOOGTEST_OPENSCOP = \
	openscop/matmult \
	openscop/empty \
	openscop/loops \
	openscop/union \
	openscop/coordinates \
	openscop/clay
endif

SPECIAL_TESTS = \
	isl/unroll \
	isl/jacobi-shared \
	isl/unroll2 \
	backtrack \
	vasilache \
	merge \
	equality \
	equality2 \
	otl \
	param-split \
	pouchet \
	stride \
	stride2 \
	sor1d

SPECIAL_OPTIONS = \
	'isl/unroll -first-unroll 1' \
	'isl/jacobi-shared -f 4 -l -1 -override -strides 1 -sh 1' \
	'isl/unroll2 -first-unroll 1' \
	'backtrack -f 1 -backtrack' \
	'vasilache -f 8 -l 9' \
	'merge -f -1' \
	'equality -f -1 -l 2 -override' \
	'equality2 -f -1 -l 4 -esp 1 -override' \
	'otl -block 1 -esp 0 -otl 0' \
	'param-split -f -1' \
	'pouchet -f 3 -l 7' \
	'stride -f -1 -strides 1' \
	'stride2 -f -1 -strides 1' \
	'sor1d -f -1'

generate:
	@echo "             /*-----------------------------------------------*"
	@echo "              *                 Generate files                *"
	@echo "              *-----------------------------------------------*/"
	@for x in $(CLOOGTEST_C) ; do \
		echo "Generate $$x.c" ; \
		$(top_builddir)/cloog $(srcdir)/$$x.cloog -o $(srcdir)/$$x.c ; \
	done ; \
	for x in $(CLOOGTEST_FORTRAN) ; do \
		echo "Generate $$x.f" ; \
		$(top_builddir)/cloog $(srcdir)/$$x.cloog -o $(srcdir)/$$x.f ; \
	done ; \
	for x in $(CLOOGTEST_STRIDED) ; do \
		echo "Generate $$x.c" ; \
		$(top_builddir)/cloog $(srcdir)/$$x.cloog \
			-strides 1 -o $(srcdir)/$$x.c ; \
	done ; \
	for x in $(CLOOGTEST_OPENSCOP) ; do \
		echo "Generate $$x.c" ; \
		$(top_builddir)/cloog $(srcdir)/$$x.scop \
		-openscop -o $(srcdir)/$$x.c ; \
	done ; \
	for line in $(SPECIAL_OPTIONS); do \
		options=`echo $$line | sed -e 's/^[^ ]* //'`; \
		x=`echo $$line | sed -e 's/ .*//'`; \
		echo "Generate file $$x ($$options)" ; \
		$(top_builddir)/cloog$(EXEEXT) $(srcdir)/$$x.cloog \
		    $$options > $(srcdir)/$$x.c ; \
	done

generate_good:
	@for x in $(FINITE_CLOOGTEST_C) ; do \
		echo "Generate $$x.good.c" ; \
		$(top_builddir)/cloog$(EXEEXT) -callable 1 $(srcdir)/$$x.cloog -o $(srcdir)/$$x.good.c ; \
	done; \
	for line in $(SPECIAL_OPTIONS); do echo $$line | while read x options; do \
		echo "Generate $$x.good.c ($$options)" ; \
		$(top_builddir)/cloog$(EXEEXT) -callable 1 $$options $(srcdir)/$$x.cloog -o $(srcdir)/$$x.good.c ; \
	done; done

valcheck: test_valgrind

CHECKER := "$(srcdir)/checker.sh"

TESTS_ENVIRONMENT = \
	builddir=$(builddir) \
	top_builddir=$(top_builddir) \
	srcdir=$(srcdir) \
	EXEXT=$(EXEEXT) \
	CHECKER=$(CHECKER) \
	COMPILE="$(COMPILE)" \
	LINK="$(LINK)" \
	FINITE_CLOOGTEST_C="$(FINITE_CLOOGTEST_C)" \
	CLOOGTEST_C="$(CLOOGTEST_C)" \
	CLOOGTEST_FORTRAN="$(CLOOGTEST_FORTRAN)" \
	CLOOGTEST_STRIDED="$(CLOOGTEST_STRIDED)" \
	CLOOGTEST_OPENSCOP="$(CLOOGTEST_OPENSCOP)" \
	SPECIAL_OPTIONS="$(SPECIAL_OPTIONS)"

test_hybrid: generate_test_advanced$(EXEEXT)
	$(TESTS_ENVIRONMENT) $(srcdir)/check_hybrid.sh;

test_valgrind:
	$(RM) -rf $(srcdir)/logs; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_c.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_fortran.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_strided.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_openscop.sh valgrind ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_special.sh valgrind;

test_regenerate:
	$(TESTS_ENVIRONMENT) $(srcdir)/check_c.sh regenerate ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_fortran.sh regenerate ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_strided.sh regenerate ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_openscop.sh regenerate ; \
	$(TESTS_ENVIRONMENT) $(srcdir)/check_special.sh regenerate

check_SCRIPTS = \
	check_c.sh \
	check_fortran.sh \
	check_strided.sh \
	check_openscop.sh \
	check_special.sh

TESTS = $(check_SCRIPTS)

EXTRA_DIST = \
	$(check_SCRIPTS) \
	$(srcdir)/checker.sh \
	$(CLOOGTEST_C:%=%.cloog) \
	$(CLOOGTEST_C:%=%.c) \
	$(FINITE_CLOOGTEST_C:%=%.good.c) \
	$(CLOOGTEST_FORTRAN:%=%.cloog) \
	$(CLOOGTEST_FORTRAN:%=%.f) \
	$(CLOOGTEST_STRIDED:%=%.cloog) \
	$(CLOOGTEST_STRIDED:%=%.c) \
	$(CLOOGTEST_OPENSCOP:%=%.scop) \
	$(CLOOGTEST_OPENSCOP:%=%.c) \
	$(SPECIAL_TESTS:%=%.cloog) \
	$(SPECIAL_TESTS:%=%.c) \
	$(SPECIAL_TESTS:%=%.good.c) \
	openscop/clay_orig.c \
	openscop/coordinates_orig.c
