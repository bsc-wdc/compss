polyincludedir=@includedir@/polylib
polyinclude_HEADERS = \
	arithmetique.h \
	arithmetic_errors.h

poly_arith = $(top_srcdir)/../source/arith
arithmetic_errors.h: $(poly_arith)/arithmetic_errors.h
	$(LN_S) $<
arithmetique.h: $(poly_arith)/arithmetique.h
	$(LN_S) $<
