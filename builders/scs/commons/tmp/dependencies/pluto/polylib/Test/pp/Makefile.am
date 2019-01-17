#
# $Id: Makefile.am,v 1.2 2005/10/20 11:35:25 verdoolaege Exp $
#

OBJ_DIR = $(top_builddir)

PP =  \
	c6.in \
	c7.in \
	salvatore.in \
	salvatore_new1.in \
	salvatore_new2.in \
	xavi.in \
	ray1.in \
	ray2.in \
	ray3.in

#	c5.in \
#	c4.in \
#	c9.in
#	c10.in

EXTRA_DIST = $(PP) \
	$(PP:%.in=%.out) 

tests:
	@failedtest=0; \
	for x in $(PP) ; do \
		echo "Verify file $$x"; \
		$(OBJ_DIR)/pp$(TEST_BITS) < $(srcdir)/$$x > xyz;\
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
	        echo "Test pp works correctly"; \
        fi
CLEANFILES=xyz
