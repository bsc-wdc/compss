#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    // Variables used per process
    int myid, numprocs;
    
    // Global variables
    char* fileName;
    int counterValue;

    //-------------------------------------------
    // INIT
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);

    //-------------------------------------------
    // We only use process 0
    if (myid == 0) {
        printf("[INFO] Increment MPI with %d processes\n", numprocs);

        // Retrieve data filename
        fileName = argv[1];
        printf("[DEBUG] FileName : %s\n", fileName);

        // Read input value
        FILE *f = fopen(fileName, "r");
        if (f == NULL) {
           printf("[ERROR] Cannot open file in read mode!\n");
           exit(1);
        }
        char readCounter [1];
        fscanf(f, "%s", readCounter);
        counterValue = atoi(readCounter);
        fclose(f);
        printf("[DEBUG] Value read: %d\n", counterValue);

        // Increment value
        counterValue = counterValue + 1;
        printf("[DEBUG] Value increased: %d\n", counterValue);


        // Write final value
        f = fopen(fileName, "w");
        if (f == NULL) {
           printf("[ERROR] Cannot open file in write mode!\n");
           exit(1);
        }

        fprintf(f, "%d", counterValue);
        fclose(f);
    }

    //-------------------------------------------
    // FINISH
    MPI_Finalize();
    printf("DONE\n");
}
