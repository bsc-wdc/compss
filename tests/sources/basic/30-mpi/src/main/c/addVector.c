#include "mpi.h"
#include <stdio.h>
#include <math.h>

int main(int argc, char **argv) {
    // Variables used per process
    int myid, numprocs;
    int myresult = 0;
    
    // Global variables
    int DATA_SIZE = argc - 1;
    int data[DATA_SIZE], result;
    
    // Chunk control variables
    int i, low, high, size;
    
    //-------------------------------------------
    // INIT
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);

    //-------------------------------------------
    // PROCESS 0 GETS THE DATA
    if (myid == 0) {
        printf("Processing data\n");
        // Get data from args
        for (i = 1; i < argc; ++i) {
          data[i - 1] = atoi(argv[i]);
        }
    }

    // Distribute the Data
    MPI_Bcast(data, DATA_SIZE, MPI_INT, 0, MPI_COMM_WORLD);
    
    //-------------------------------------------
    // EACH PROCESS COMPUTES ITS PART
    size = DATA_SIZE/numprocs;
    low = myid * size;
    high = low + size;
    for (i = low; i < high; ++i) {
        myresult += data[i];
    }
    printf("Process %d gets from %d to %d with result %d\n", myid, low, high, myresult);

    //-------------------------------------------
    // COMPUTE GLOBAL SUM
    MPI_Reduce(&myresult, &result, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    //-------------------------------------------
    // PROCESS 0 PRINTS THE RESULT
    if (myid == 0) {
        printf("%d\n", result);
    }

    //-------------------------------------------
    // FINISH
    MPI_Finalize();
}