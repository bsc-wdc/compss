#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
// #define DEBUG_BINDING


#include "TestArrays.h"

//For testing purposes
/*float *test2(int bSize){
        float *array = new float[bSize];
        for (int i= 0; i<bSize; i++){
                array[i]=1.0;
                std::cout << i << ":" << array[i] << std::endl;
        }
        return array;
}*/

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
	float *array1 = new float[MAX];
	printf("Array 1: %p\n",array1);
	int *array2 = new int[size];
	printf("Array 2: %p\n",array2);
	for (int i=0; i<size;i++){
		array2[i]=i;
        }
	double *array3 = new double[30];
        for (int i=0; i<30;i++){
                array3[i]=1.0;
        }
	printf("Array 3: %p\n",array3);
	char *file = "hola.txt";
	Block::test1(size, array1, array2, array3, file);
	printf("Array 1(after): %p\n",array1);
	float *array4 = test2(size);
	printf("Array 4(returned): %p\n",array4);
	Block *a = new Block();
	//Block::generate(a, 2, 1.0);
	a->test3(size, array4);
	printf("Array 4(after): %p\n",array4);
	test4(array1);
	compss_wait_on(a);
	compss_wait_on(array4);
	printf("Array 4(after wait_on): %p\n",array4);
	if (array4[0]!=1.0f){
	      printf("ERROR: Generated array4 not valid. Value is %f\n", array4[0]);
	      sleep(60);
              compss_off();
	      return -1;
        }
	compss_wait_on(array1);
	if (array1[MAX-1]!= 9.0f){
              printf("Generated array1 not valid. Value is %f\n", array1[MAX-1]);
              compss_off();
	      return -1;
        }
	compss_wait_on(array3);
 	if (array3[0]!=2.0f){
		printf("Generated arra3 not valid.Value is %f\n", array3[0]);
		compss_off();
        	return -1;
        }

	compss_off();

	return 0;
}
