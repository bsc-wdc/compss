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
	
	Block *a = new Block();
	Block *b = new Block();	
	// Log init test
        printf("[   TEST] Test static call.\n");

	// Char (char_dt, wchar_dt)

	Block::generate(a, 2, 1.0);
        Block::generate(b, 2, 2.0);
	compss_wait_on(*b);
	if (b->data[0][0]!=2.0){
		printf("Generated object not valid");
		return -1;
	}
	compss_wait_on(*a);
        if (a->data[0][0]!=1.0){
                printf("Generated object not valid");
                return -1;
        }
	
	Block c = Block::create(2, 0.0);
	compss_wait_on(c);
	if (c.data[0][0]!= 0.0){
                printf("Generated object not valid");
                return -1;
        }
        c.multiply(a,b);
	compss_wait_on(c);
	if (c.data[0][0]!= 4.0){
                printf("Generated object not valid");
                return -1;
        }
	c.print();
	compss_off();

	return 0;
}
