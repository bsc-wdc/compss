SUBDIRS = polylib

AM_CPPFLAGS = -I$(srcdir)/../include -I. -DCLN

poly_src = $(srcdir)/../applications

lib_LTLIBRARIES = libpolylibcln.la
LDADD = libpolylibcln.la

noinst_PROGRAMS = \
	testlib \
	example \
	count \
	Zpolytest \
	polytest \
	verif_ehrhart

bin_PROGRAMS = \
	c2pcln \
	r2pcln \
	findvcln \
	disjoint_union_sepcln \
	disjoint_union_adjcln \
	ehrhart_quick_apxcln \
	ehrhart_upper_boundcln \
	ehrhart_lower_boundcln \
	ehrhart_rankingcln \
	ehrhart_unioncln \
	ppcln \
	testehrhartcln

CFILES= \
	../source/arith/errors.c \
	../source/kernel/errormsg.c \
	../source/kernel/vector.c \
	../source/kernel/matrix.c \
	../source/kernel/polyhedron.c \
	../source/kernel/polyparam.c \
	../source/kernel/param.c \
	../source/kernel/alpha.c \
	../source/ehrhart/ehrhart.c \
	../source/ehrhart/ext_ehrhart.c \
	../source/ehrhart/eval_ehrhart.c \
	../source/ehrhart/homogenization.c \
	../source/ehrhart/ranking.c \
	../source/kernel/matrix_addon.c \
	../source/kernel/matrix_permutations.c \
	../source/kernel/compress_parms.c \
	../source/kernel/SolveDio.c \
	../source/kernel/Lattice.c \
	../source/kernel/Matop.c \
	../source/kernel/NormalForms.c \
	../source/kernel/Zpolyhedron.c		

libpolylibcln_la_SOURCES = $(CFILES)
# trick to force use of c++ linker
nodist_EXTRA_libpolylibcln_la_SOURCES = dummy.cxx

testlib_SOURCES = $(poly_src)/testlib.c
example_SOURCES = $(poly_src)/example.c
count_SOURCES = $(srcdir)/../source/count.c
Zpolytest_SOURCES = $(poly_src)/Zpolytest.c
ehrhart_rankingcln_SOURCES = $(poly_src)/ehrhart_ranking.c
ehrhart_unioncln_SOURCES = $(poly_src)/ehrhart_union.c
polytest_SOURCES = $(poly_src)/polytest.c
ppcln_SOURCES = $(poly_src)/pp.c
testehrhartcln_SOURCES = $(poly_src)/testehrhart.c
verif_ehrhart_SOURCES = $(poly_src)/verif_ehrhart.c

c2pcln_SOURCES = $(poly_src)/c2p.c
r2pcln_SOURCES = $(poly_src)/r2p.c
findvcln_SOURCES = $(poly_src)/findv.c
disjoint_union_sepcln_SOURCES = $(poly_src)/disjoint_union_sep.c
disjoint_union_adjcln_SOURCES = $(poly_src)/disjoint_union_adj.c
ehrhart_quick_apxcln_SOURCES = $(poly_src)/ehrhart_quick_apx.c
ehrhart_upper_boundcln_SOURCES =  $(poly_src)/ehrhart_upper_bound.c
ehrhart_lower_boundcln_SOURCES =  $(poly_src)/ehrhart_lower_bound.c

# hack to compile c source files using c++ compiler
CC=$(CXX)

ACLOCAL_AMFLAGS = -I m4

ZPOLYTESTS =  \
	LatHNF1.in \
	LatHNF2.in \
	LatHNF3.in \
	LatInter1.in \
	LatInter2.in \
	LatInc1.in \
	LatInc2.in \
	ZEmpty1.in \
	ZEmpty2.in \
	ZInter1.in \
	ZInter2.in  \
	ZInter3.in  \
	ZUnion1.in \
	ZUnion2.in \
	ZDiff1.in \
	ZDiff2.in \
	ZImage1.in \
	ZPre1.in \
	ZPre2.in  \
	ZPre3.in \
	ZImPre1.in \
	SplitZpol1.in \
	SplitZpol2.in \
	SplitZpol3.in

EHRHART =  \
	0D.in \
	0D2.in \
	8hedron.in \
	alex11.in \
	alex1.in \
	bart6.in \
	bart8.in \
	c3.in\
	c4.in\
	e0.in\
	e10.in\
	e11.in\
	e12.in\
	e13.in\
	e14.in\
	e15.in\
	e16.in\
	e1.in\
	e2b.in\
	e2.in\
	e3.in\
	e4.in\
	e5.in\
	e6.in\
	e7.in\
	e8.in\
	e9.in\
	esum.in \
	g12.in\
	g13.in\
	g14.in\
	g1.in\
	g2.in\
	g4.in\
	g5.in\
	isnm2.in\
	t1.in\
	t2.in\
	t31.in\
	t3.in\
	t4.in\
	t12.in\
	tpar.in

EHRHART_UNION =  \
	ue1.in \
	ue2.in

RANKING =  \
	asci03-core.in

GENERAL =  \
	convex1.in \
	convex2.in \
	gauss1.in \
	herve.in \
	simpl1.in \
	simpl2.in \
	simpl3.in \
	simpl4.in \
	simpl5.in \
	simpl6.in \
	simpl7.in \
	test1.in

PP =  \
	c6.in \
	c7.in

check:
	@failedtest=0; \
	for x in $(ZPOLYTESTS) ; do \
		echo "Verify file $$x" ; \
		./Zpolytest$(TEST_BITS) < $(srcdir)/../Test/Zpolytest/$$x > xyz;\
		diff -w xyz $(srcdir)/../Test/Zpolytest/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test Zpolytest works correctly"; \
        fi
	@failedtest=0; \
	for x in $(EHRHART) ; do \
		echo "Verify file $$x" ; \
		./testehrhartcln < $(srcdir)/../Test/ehrhart/$$x >xyz; \
		diff -w xyz $(srcdir)/../Test/ehrhart/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test ehrhart works correctly"; \
        fi
	@failedtest=0; \
	for x in $(EHRHART_UNION) ; do \
		echo "Verify file $$x" ; \
		./ehrhart_unioncln < $(srcdir)/../Test/ehrhart_union/$$x > xyz;\
		diff -w xyz $(srcdir)/../Test/ehrhart_union/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test ehrhart_union works correctly"; \
        fi
	@failedtest=0; \
	for x in $(RANKING) ; do \
		echo "Verify file $$x" ; \
		./ehrhart_rankingcln < $(srcdir)/../Test/ranking/$$x >xyz; \
		diff -w xyz $(srcdir)/../Test/ranking/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test ehrhart_ranking works correctly"; \
        fi
	@failedtest=0; \
	for x in $(GENERAL) ; do \
		echo "Verify file $$x"; \
		./polytest < $(srcdir)/../Test/general/$$x > xyz;\
		diff -w xyz $(srcdir)/../Test/general/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test polytest works correctly"; \
        fi
	@failedtest=0; \
	for x in $(PP) ; do \
		echo "Verify file $$x"; \
		./ppcln < $(srcdir)/../Test/pp/$$x > xyz;\
		diff -w xyz $(srcdir)/../Test/pp/`basename $$x .in`.out ; \
		result=$$?; \
		if [ "$$result" -eq "1" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
        if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "Test pp works correctly"; \
        fi

longtest:
	@for x in $(EHRHART) ; do \
		echo "Verify Ehrhart for $$x"; \
		./verif_ehrhart < $(srcdir)/../Test/ehrhart/$$x ;\
	done ;
