#
# $Id: Makefile.am,v 1.3 2007/02/21 21:26:35 skimo Exp $
#

OBJ_DIR = $(top_builddir)

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
	simpl8.in \
	simpl9.in \
	test1.in

EXTRA_DIST = $(GENERAL) \
	$(GENERAL:%.in=%.out) 

#	test.in

tests:
	@failedtest=0; \
	for x in $(GENERAL) ; do \
		echo "Verify file $$x"; \
		$(OBJ_DIR)/polytest$(TEST_BITS) < $(srcdir)/$$x > xyz;\
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
	        echo "Test polytest works correctly"; \
        fi
CLEANFILES=xyz
