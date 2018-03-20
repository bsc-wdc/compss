#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

// #define DEBUG_BINDING


#include "TestArrays.h"


int main(int argc, char **argv)
{
	printf("*--------------------------------------------------------------------*\n");
	printf("*                                                                    *\n");
	printf("*     Test for COMP Superscalar C/C++ Binding...                     *\n");
	printf("*                                                                    *\n");
	printf("*     This test will test methods with array parameters.             *\n");
	printf("*                                                                    *\n");
	printf("*                                                                    *\n");
	printf("*--------------------------------------------------------------------*\n");
	printf("\n");


	compss_on();

	printf("[   TEST] Test static call.\n");

	int size = 20;
	float *array1;
	int *array2 = new int[size];
	double *array3 = new double[30];

	Block::test1(size, array1, array2, array3);

	float *array4 = test2(size);
	Block *a = new Block();
	a->test3(size, array4);
	test4(array1);

	//compss_wait_on(array4);
	//if (array4[0] == xx){
	//	printf("Generated object not valid");
        //      return -1;
        //}

 	//compss_wait_on(array3);
	//if (array3[0] == xx){
        //      printf("Generated object not valid");
        //      return -1;
        //}
	//compss_wait_on(array1);
 	//if (array1[0] == xx){
        //      printf("Generated object not valid");
        //      return -1;
        //}

	compss_off();

	return 0;
}
