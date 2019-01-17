noinst_PROGRAMS = @examples@
EXTRA_PROGRAMS = example_sp example_dp example_gmp example_osl

example_sp_SOURCES = example.c
example_sp_CPPFLAGS = @SP_DFLAGS@ -I$(top_srcdir)/include
example_sp_LDADD = ../libpiplib_sp.la
example_dp_SOURCES = example.c
example_dp_CPPFLAGS = @DP_DFLAGS@ -I$(top_srcdir)/include
example_dp_LDADD = ../libpiplib_dp.la
example_gmp_SOURCES = example.c
example_gmp_CPPFLAGS = @MP_DFLAGS@ -I$(top_srcdir)/include
example_gmp_LDADD = ../libpiplib_gmp.la @EXTRA_LIBS@
example_osl_SOURCES = example.c
example_osl_CPPFLAGS = @OSL_DFLAGS@ -I$(top_srcdir)/include
example_osl_LDADD = ../libpiplib_osl.la @EXTRA_LIBS@

PIPTEST = \
	big \
	boulet \
	brisebarre \
	cg1 \
	esced \
	ex \
	ex2 \
	expansion \
	fimmel \
	max \
	negative \
	small \
	sor1d \
	square \
	sven

EXTRA_DIST = \
	$(PIPTEST:%=%.pip) \
	$(PIPTEST:%=%.ll)

test:
	@failedtest=0; \
	for x in $(PIPTEST) ; do \
		echo "Verify file $(srcdir)/$$x" ; \
		./$(EXAMPLE) < $(srcdir)/$$x.pip > xyz ;\
		diff -w xyz $(srcdir)/$$x.ll ; \
		result=$$?; \
		if [ "$$result" -ne "0" ]; then \
			echo "Error: $$x is not the same"; \
			failedtest=`expr $$failedtest + 1`; \
		else \
			echo "$$x passed"; \
	        fi; \
	done ; \
	rm xyz ; \
	if [ $$failedtest != 0 ]; then \
		echo "$$failedtest tests failed"; \
	else \
	        echo "PIP works correctly :-)"; \
        fi
