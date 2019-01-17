#
# $Id: Makefile.am,v 1.2 2005/10/20 11:35:25 verdoolaege Exp $
#

OBJ_DIR = $(top_builddir)

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

EXTRA_DIST = $(ZPOLYTESTS) \
	$(ZPOLYTESTS:%.in=%.out) 

TEST=$(ZPOLYTESTS:%.in=%.tested) 

tests:
	@failedtest=0; \
	for x in $(ZPOLYTESTS) ; do \
		echo "Verify file $$x" ; \
		$(OBJ_DIR)/Zpolytest$(TEST_BITS) < $(srcdir)/$$x > xyz;\
		diff -w xyz $(srcdir)/`basename $$x .in`.out ; \
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
CLEANFILES=xyz
