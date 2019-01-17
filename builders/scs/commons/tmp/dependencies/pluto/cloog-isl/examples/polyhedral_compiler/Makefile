# Please enter here the locations for the CLooG, OpenScop and Clan
# include and library if they aren't the default values
# (i.e., /usr/lib and /usr/include).
CLOOG_INC = $(HOME)/usr/include
CLOOG_LIB = $(HOME)/usr/lib

OPENSCOP_INC = $(HOME)/usr/include
OPENSCOP_LIB = $(HOME)/usr/lib

CLAN_INC = $(HOME)/usr/include
CLAN_LIB = $(HOME)/usr/lib

CC = gcc
LDLIBS= -lcloog-isl -losl -lclan
CFLAGS= -DCLOOG_INT_GMP \
        -I $(CLOOG_INC)    -L $(CLOOG_LIB) \
        -I $(OPENSCOP_INC) -L $(OPENSCOP_LIB) \
        -I $(CLAN_INC)     -L $(CLAN_LIB)

example: poc.c 
	@echo "             /*-----------------------------------------------*"
	@echo "              *        Making Polyhedral Compiler Example     *"
	@echo "              *-----------------------------------------------*/"
	$(CC) poc.c -o poc $(CFLAGS) $(LDLIBS)

clean:
	@echo "             /*-----------------------------------------------*"
	@echo "              *     Cleaning Polyhedral Compiler Example      *"
	@echo "              *-----------------------------------------------*/"
	-rm -f poc poc.exe core
