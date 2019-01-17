#
# $Id: Makefile.am,v 1.2 2005/10/20 11:35:25 verdoolaege Exp $
#

OBJ_DIR = $(top_builddir)

RANKING =  \
	asci03-core.in

EXTRA_DIST = $(RANKING) \
	$(RANKING:%.in=%.out) 

tests:
	@failedtest=0; \
	for x in $(RANKING) ; do \
		echo "Verify file $$x" ; \
		$(OBJ_DIR)/ehrhart_ranking$(TEST_BITS) < $(srcdir)/$$x >xyz; \
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
	        echo "Test ehrhart_ranking works correctly"; \
        fi
CLEANFILES=xyz
