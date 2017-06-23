#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

// #define DEBUG_BINDING


#include "TestStatic.h"


int main(int argc, char **argv)
{
	printf("*--------------------------------------------------------------------*\n");
	printf("*                                                                    *\n");
	printf("*     Test for COMP Superscalar C/C++ Binding...                     *\n");
	printf("*                                                                    *\n");
	printf("*     This test will test class static methods           .           *\n");
	printf("*                                                                    *\n");
	printf("*                                                                    *\n");
	printf("*--------------------------------------------------------------------*\n");
	printf("\n");


	compss_on();
	
	Block *b = new Block();
		
	// Log init test
        printf("[   TEST] Test static call.\n");

	// Char (char_dt, wchar_dt)

	Block::generate(b, 2, 12.3);
	compss_wait_on(b);
	if (b->data[0][0]!=12.3){
		printf("Generated object not valid");
		return -1;
	}
	Block c = Block::create(2,10.0);
	compss_wait_on(c);
	if (b->data[0][0]!=12.3){
                printf("Generated object not valid");
                return -1;
        }
	compss_off();

	return 0;
}
