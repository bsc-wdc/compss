
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include "executor.h"

int main(int argc, char **argv) {
    if (argc < 4) {
        printf("ERROR: Incorrect number of COMPSs internal parameters\n");
        printf("Aborting...\n");
        return -1;
    }
    //Reading in pipes
    printf("numInPipes %s.\n", argv[1]);
    int numInPipes=atoi(argv[1]);
    printf("Detected %d in pipes.\n", numInPipes);
    char* inPipes[numInPipes];
    for (int i=0; i<numInPipes; i++){
        inPipes[i] = argv[i+2];
        printf("In pipe %d: %s\n",i, inPipes[i]);
    }
    //Reading out pipes
    printf("numInPipes %s.\n", argv[numInPipes+2]);
    int numOutPipes=atoi(argv[numInPipes+2]);
    printf("Detected %d out pipes.\n", numOutPipes);
    char* outPipes[numOutPipes];
    for (int i=0; i<numOutPipes; i++){
        outPipes[i] = argv[i+numInPipes+3];
	printf("Out pipe %d: %s\n",i, outPipes[i]);
    }
    
    //Add here treads stuff
    
    return 0;
} 
