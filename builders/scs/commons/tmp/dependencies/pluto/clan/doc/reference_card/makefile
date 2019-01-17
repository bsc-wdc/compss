NAME_SOURCE=reference_card

all:
	texi2pdf $(NAME_SOURCE).tex

clean:
	rm -f $(NAME_SOURCE).aux $(NAME_SOURCE).log $(NAME_SOURCE).dvi \
	$(NAME_SOURCE).bbl $(NAME_SOURCE).blg

cleanall: clean
	rm -f $(NAME_SOURCE).pdf
