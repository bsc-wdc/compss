SUBDIRS = include Test

EXTRA_CPPFLAGS = -I$(srcdir)/include -Iinclude @CPPFLAGS@

ACLOCAL_AMFLAGS = -I m4

poly_src = $(srcdir)/applications
poly_arith = $(srcdir)/source/arith

CFILES= \
	source/arith/errors.c \
	source/kernel/errormsg.c \
	source/kernel/vector.c \
	source/kernel/matrix.c \
	source/kernel/polyhedron.c \
	source/kernel/polyparam.c \
	source/kernel/param.c \
	source/kernel/alpha.c \
	source/ehrhart/ehrhart.c \
	source/ehrhart/ext_ehrhart.c \
	source/ehrhart/eval_ehrhart.c \
	source/ehrhart/homogenization.c \
	source/ehrhart/ranking.c \
	source/kernel/matrix_addon.c \
	source/kernel/matrix_permutations.c \
	source/kernel/compress_parms.c \
	source/kernel/SolveDio.c \
	source/kernel/Lattice.c \
	source/kernel/Matop.c \
	source/kernel/NormalForms.c \
	source/kernel/Zpolyhedron.c		

sources = $(CFILES) \
	$(poly_arith)/assert.h \
	$(poly_arith)/arithmetique.h \
	$(poly_arith)/arithmetic_errors.h

EXTRA_DIST = doc source/oldpolytest.c INSTALL COPYING m4/acx_pthread.m4

lib_LTLIBRARIES = @polylibs@
libpolylib32_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpolylib32_la_SOURCES = $(sources)
libpolylib32_la_CPPFLAGS = -DPOLYLIB_BITS=32 $(EXTRA_CPPFLAGS)
libpolylib32_la_LIBADD = @LTLIBOBJS@
libpolylib64_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpolylib64_la_SOURCES = $(sources)
libpolylib64_la_CPPFLAGS = -DPOLYLIB_BITS=64 $(EXTRA_CPPFLAGS)
libpolylib64_la_LIBADD = @LTLIBOBJS@
libpolylib128_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpolylib128_la_SOURCES = $(sources)
libpolylib128_la_CPPFLAGS = -DPOLYLIB_BITS=128  $(EXTRA_CPPFLAGS)
libpolylib128_la_LIBADD = @LTLIBOBJS@
libpolylibgmp_la_LDFLAGS = -rpath $(libdir) -version-info @versioninfo@
libpolylibgmp_la_SOURCES = $(sources)
libpolylibgmp_la_CPPFLAGS = -DGNUMP  $(EXTRA_CPPFLAGS)
libpolylibgmp_la_LIBADD = @LTLIBOBJS@

testlib_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
example_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
count_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
c2p_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
r2p_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
findv_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
disjoint_union_sep_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
disjoint_union_adj_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
ehrhart_quick_apx_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
ehrhart_lower_bound_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)
ehrhart_upper_bound_CPPFLAGS = $(libpolylib@BITS@_la_CPPFLAGS)


EXTRA_LTLIBRARIES = libpolylib32.la libpolylib64.la libpolylib128.la \
		    libpolylibgmp.la

# not using foreach GNU make extension
no_inst_programs_32=Zpolytest32$(EXEEXT) polytest32$(EXEEXT) verif_ehrhart32$(EXEEXT) testCompressParms32$(EXEEXT)
no_inst_programs_64=Zpolytest64$(EXEEXT) polytest64$(EXEEXT) verif_ehrhart64$(EXEEXT) testCompressParms64$(EXEEXT)
no_inst_programs_gmp=Zpolytestgmp$(EXEEXT) polytestgmp$(EXEEXT) verif_ehrhartgmp$(EXEEXT) testCompressParmsgmp$(EXEEXT)

noinst_PROGRAMS = \
	testlib \
	example \
	count \
	@multi_noinst_programs@

multi_bin_programs_32=ehrhart_ranking32$(EXEEXT) ehrhart_union32$(EXEEXT) pp32$(EXEEXT) testehrhart32$(EXEEXT)
multi_bin_programs_64=ehrhart_ranking64$(EXEEXT) ehrhart_union64$(EXEEXT) pp64$(EXEEXT) testehrhart64$(EXEEXT)
multi_bin_programs_gmp=ehrhart_rankinggmp$(EXEEXT) ehrhart_uniongmp$(EXEEXT) ppgmp$(EXEEXT) testehrhartgmp$(EXEEXT)

bin_PROGRAMS = \
	c2p \
	r2p \
	findv \
	disjoint_union_sep \
	disjoint_union_adj \
	ehrhart_quick_apx \
	ehrhart_upper_bound \
	ehrhart_lower_bound \
	@multi_bin_programs@

EXTRA_PROGRAMS = $(no_inst_programs_32) $(no_inst_programs_64) $(no_inst_programs_gmp) \
				 $(multi_bin_programs_32) $(multi_bin_programs_64) $(multi_bin_programs_gmp)



LDADD = @polylib@
c2p_SOURCES = $(poly_src)/c2p.c
c2p_DEPENDENCIES = @polylib@
r2p_SOURCES = $(poly_src)/r2p.c
r2p_DEPENDENCIES = @polylib@
findv_SOURCES = $(poly_src)/findv.c
findv_DEPENDENCIES = @polylib@
disjoint_union_sep_SOURCES = $(poly_src)/disjoint_union_sep.c
disjoint_union_sep_DEPENDENCIES = @polylib@
disjoint_union_adj_SOURCES = $(poly_src)/disjoint_union_adj.c
disjoint_union_adj_DEPENDENCIES = @polylib@
ehrhart_quick_apx_SOURCES = $(poly_src)/ehrhart_quick_apx.c
ehrhart_quick_apx_DEPENDENCIES = @polylib@
ehrhart_upper_bound_SOURCES =  $(poly_src)/ehrhart_upper_bound.c
ehrhart_upper_bound_DEPENDENCIES = @polylib@
ehrhart_lower_bound_SOURCES =  $(poly_src)/ehrhart_lower_bound.c
ehrhart_lower_bound_DEPENDENCIES = @polylib@
testlib_SOURCES = $(poly_src)/testlib.c
testlib_DEPENDENCIES = @polylib@
example_SOURCES = $(poly_src)/example.c
example_DEPENDENCIES = @polylib@
count_SOURCES = source/count.c
count_DEPENDENCIES = @polylib@
Zpolytest32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
Zpolytest32_SOURCES = $(poly_src)/Zpolytest.c
Zpolytest32_LDADD = libpolylib32.la
Zpolytest64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
Zpolytest64_SOURCES = $(poly_src)/Zpolytest.c
Zpolytest64_LDADD = libpolylib64.la
Zpolytestgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
Zpolytestgmp_SOURCES = $(poly_src)/Zpolytest.c
Zpolytestgmp_LDADD = libpolylibgmp.la
ehrhart_ranking32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
ehrhart_ranking32_SOURCES = $(poly_src)/ehrhart_ranking.c
ehrhart_ranking32_LDADD = libpolylib32.la
ehrhart_ranking64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
ehrhart_ranking64_SOURCES = $(poly_src)/ehrhart_ranking.c
ehrhart_ranking64_LDADD = libpolylib64.la
ehrhart_rankinggmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
ehrhart_rankinggmp_SOURCES = $(poly_src)/ehrhart_ranking.c
ehrhart_rankinggmp_LDADD = libpolylibgmp.la
ehrhart_union32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
ehrhart_union32_SOURCES = $(poly_src)/ehrhart_union.c
ehrhart_union32_LDADD = libpolylib32.la
ehrhart_union64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
ehrhart_union64_SOURCES = $(poly_src)/ehrhart_union.c
ehrhart_union64_LDADD = libpolylib64.la
ehrhart_uniongmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
ehrhart_uniongmp_SOURCES = $(poly_src)/ehrhart_union.c
ehrhart_uniongmp_LDADD = libpolylibgmp.la
polytest32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
polytest32_SOURCES = $(poly_src)/polytest.c
polytest32_LDADD = libpolylib32.la
polytest64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
polytest64_SOURCES = $(poly_src)/polytest.c
polytest64_LDADD = libpolylib64.la
polytestgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
polytestgmp_SOURCES = $(poly_src)/polytest.c
polytestgmp_LDADD = libpolylibgmp.la
pp32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
pp32_SOURCES = $(poly_src)/pp.c
pp32_LDADD = libpolylib32.la
pp64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
pp64_SOURCES = $(poly_src)/pp.c
pp64_LDADD = libpolylib64.la
ppgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
ppgmp_SOURCES = $(poly_src)/pp.c
ppgmp_LDADD = libpolylibgmp.la
testehrhart32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
testehrhart32_SOURCES = $(poly_src)/testehrhart.c
testehrhart32_LDADD = libpolylib32.la
testehrhart64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
testehrhart64_SOURCES = $(poly_src)/testehrhart.c
testehrhart64_LDADD = libpolylib64.la
testehrhartgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
testehrhartgmp_SOURCES = $(poly_src)/testehrhart.c
testehrhartgmp_LDADD = libpolylibgmp.la
testCompressParms32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
testCompressParms32_SOURCES = $(poly_src)/testCompressParms.c
testCompressParms32_LDADD = libpolylib32.la
testCompressParms64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
testCompressParms64_SOURCES = $(poly_src)/testCompressParms.c
testCompressParms64_LDADD = libpolylib64.la
testCompressParmsgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
testCompressParmsgmp_SOURCES = $(poly_src)/testCompressParms.c
testCompressParmsgmp_LDADD = libpolylibgmp.la
verif_ehrhart32_CPPFLAGS = $(libpolylib32_la_CPPFLAGS)
verif_ehrhart32_SOURCES = $(poly_src)/verif_ehrhart.c
verif_ehrhart32_LDADD = libpolylib32.la
verif_ehrhart64_CPPFLAGS = $(libpolylib64_la_CPPFLAGS)
verif_ehrhart64_SOURCES = $(poly_src)/verif_ehrhart.c
verif_ehrhart64_LDADD = libpolylib64.la
verif_ehrhartgmp_CPPFLAGS = $(libpolylibgmp_la_CPPFLAGS)
verif_ehrhartgmp_SOURCES = $(poly_src)/verif_ehrhart.c
verif_ehrhartgmp_LDADD = libpolylibgmp.la

check: tests

tests: all
	(cd Test; $(MAKE) tests)

longtest: all
	(cd Test; $(MAKE) long_tests)

# Make the 'javadoc' style documentation
document:
	-mkdir doc
	$(DOXYGEN) polylib.doxygen

dist-hook: document
	rm -rf `find $(distdir)/doc -name CVS`

clean-local:

distclean-local:
	rm -f polylib*.pc polylib*-uninstalled.pc polylib*-uninstalled.sh polylib*.pc.in

install-data-local:
	@test -z "$(pkgconfig_libdir)" || $(mkdir_p) "$(DESTDIR)$(pkgconfig_libdir)"
	$(INSTALL_DATA) $(pkgconfig_libfile) "$(DESTDIR)$(pkgconfig_libdir)/$(pkgconfig_libfile)"

uninstall-local:
	rm -f "$(DESTDIR)$(pkgconfig_libdir)/$(pkgconfig_libfile)"
