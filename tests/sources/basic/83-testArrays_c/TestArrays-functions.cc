#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include "TestArrays.h"

static float *test2(int bSize){
	float *array = new Float[bSize];
	for (int i= 0; i<bSize; i++){
		array[i]=1.0;
	}
	return array;
}


float *test4(in float* in_array){

	float *array = new Float[20];
	for (int i= 0; i<20; i++){
                array[i]=in_array[i];
        }
	return array;
}
