#include "mpi.h"
#include <stdio.h>
#include <math.h>
#include <string.h>

int main(int argc, char **argv) {
    // Variables used per process
    int myid, numprocs;
    
    // Global variables
    char* msg;
    int length;
    
    //-------------------------------------------
    // INIT
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);

    //-------------------------------------------
    // PROCESS 0 GETS THE DATA
    if (myid == 0) {
        printf("Processing data\n");
        // Get message from args
        msg = argv[1];
        length = sizeof(msg);
    }

    // Distribute the Data
    MPI_Bcast(&msg, length, MPI_CHAR, 0, MPI_COMM_WORLD);
    
    //-------------------------------------------
    // EACH PROCESS PRINTS MESSAGE
    printf("Process %d says %s\n", myid, msg);

    //-------------------------------------------
    // FINISH
    MPI_Finalize();
}