EXTRA_DIST = cases old others
SUBDIRS = Zpolytest ehrhart ehrhart_union ranking general pp

tests:
	@for BITS in $(ALL_BITS); do \
	    echo "Performing $$BITS tests"; \
	    for d in $(SUBDIRS); do \
		(cd $$d; $(MAKE) TEST_BITS=$$BITS tests) \
	    done \
	done

long_tests:
	@for BITS in $(ALL_BITS); do \
	    echo "Performing $$BITS tests"; \
	    (cd ehrhart; $(MAKE) TEST_BITS=$$BITS longtest) \
	done

dist-hook:
	rm -rf `find $(distdir) -name CVS`
