#
# $Id: Makefile.am,v 1.2 2005/10/20 11:35:25 verdoolaege Exp $
#

OBJ_DIR = $(top_builddir)

EHRHART_UNION =  \
	ue1.in \
	ue2.in

EXTRA_DIST = $(EHRHART_UNION) \
	$(EHRHART_UNION:%.in=%.out) 

TEST=$(EHRHART_UNION:%.in=%.tested) 

tests:
	@failedtest=0; \
	for x in $(EHRHART_UNION) ; do \
		echo "Verify file $$x" ; \
		$(OBJ_DIR)/ehrhart_union$(TEST_BITS) < $(srcdir)/$$x > xyz;\
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
	        echo "Test ehrhart_union works correctly"; \
        fi
CLEANFILES=xyz
