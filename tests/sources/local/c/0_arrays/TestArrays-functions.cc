#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <iostream>
#include <iomanip>
#include "TestArrays.h"

float *test2(int bSize){
	float *array = new float[bSize];
	for (int i= 0; i<bSize; i++){
		array[i]=1.0;
		std::cout << i << ":" << array[i] << std::endl;
	}
	return array;
}


float *test4(float* in_array){

	float *array = new float[20];
	for (int i= 0; i<20; i++){
                array[i]=in_array[i];
		std::cout << i << ":" << array[i] << std::endl;
        }
	return array;
}
