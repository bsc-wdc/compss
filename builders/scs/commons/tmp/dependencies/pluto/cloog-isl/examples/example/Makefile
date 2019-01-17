# Please enter here the locations for the CLooG include and library if they
# aren't the default values (/usr/lib and /usr/include).
CLOOG_INC = $(HOME)/usr/include
CLOOG_LIB = $(HOME)/usr/lib

CC = gcc
LDLIBS= -lcloog-isl
CFLAGS= -DCLOOG_INT_GMP -I $(CLOOG_INC) -L $(CLOOG_LIB)

example: example.c example-isl.c 
	@echo "             /*-----------------------------------------------*"
	@echo "              *                Making examples                *"
	@echo "              *-----------------------------------------------*/"
	$(CC) example.c -o example $(CFLAGS) $(LDLIBS)
	$(CC) example-isl.c -o example-isl $(CFLAGS) $(LDLIBS)

clean:
	@echo "             /*-----------------------------------------------*"
	@echo "              *               Cleaning examples               *"
	@echo "              *-----------------------------------------------*/"
	-rm -f example example.exe example-isl example-isl.exe core
