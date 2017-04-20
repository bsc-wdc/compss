#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define MASTER 0               /* taskid of first task */
#define FROM_MASTER 1          /* setting a message type */
#define FROM_WORKER 2          /* setting a message type */

void fillMatrix(const char* fileName, int matrixSize, double* mat) {
    int i, j;
    
    FILE *file;
    file = fopen(fileName, "r");

    // printf("  - Open file %s with size %d.\n", fileName, matrixSize);
    for(i = 0; i < matrixSize; i++) {
        for(j = 0; j < matrixSize; j++) {
            if (!fscanf(file, "%lf", &mat[i*matrixSize + j])) {
                break;
            }
            // printf("%lf\n", mat[i*matrixSize + j]);
        }
    }
    fclose(file);    
}

void storeMatrix(const char* fileName, int matrixSize, const double* mat) {
    int i, j;

    FILE *file;
    file = fopen(fileName, "w");

    // printf("  - Store matrix in file %s with size %d.\n", fileName, matrixSize);
    for(i = 0; i < matrixSize; i++) {
        for(j = 0; j < matrixSize; j++) {
            fprintf(file, "%lf ", mat[i*matrixSize + j]);
            // printf("%lf\n", mat[i*matrixSize + j]);
        }
        fprintf(file, "\n");
    }
    fclose(file);
}

void printMatrix(int matrixSize, const double* mat) {
    int i, j;
    
    // printf("******************************************************\n");
    // printf("Result Matrix:\n");
    for (i = 0; i < matrixSize; i++) {
        // printf("\n"); 
        for (j = 0; j < matrixSize; j++) {
            // printf("%6.2f   ", mat[i*matrixSize + j]);
        }
    }
    // printf("\n******************************************************\n");
}

int main (int argc, char *argv[]) {    
    /**************************** Initialize ************************************/
    if (argc != 5) {
        // printf("Incorrect usage: matmul <matrixSize> <Ain> <Bin> <Cout>. Quitting...\n");
        exit(1);
    }
    int matrixSize = atoi(argv[1]);     // Number of rows/columns in matrix A B and C
    char* ain = argv[2];                // FileName of Ain
    char* bin = argv[3];                // FileName of Bin
    char* cout = argv[4];               // FileName of Cout
    
    double a[matrixSize*matrixSize];   // Matrix A to be multiplied
    double b[matrixSize*matrixSize];   // Matrix B to be multiplied
    double c[matrixSize*matrixSize];   // Result matrix C    
    
    // Initialize MPI env
    int mpiProcs;                       // Number of MPI Nodes
    int taskid;                         // Task identifier
    MPI_Status status;                  // Status variable for MPI communications
    MPI_Request send_request;           // For async calls

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &taskid);
    MPI_Comm_size(MPI_COMM_WORLD, &mpiProcs);
    
    // Misc variables
    int i, j, k, dest, rows;
    struct timeval tInit, tFill, tSend, tRecv, tRecvDone, tRecvMaster, tRecvMasterDone, tComp, tStore;
    unsigned long long t;
    
    /**************************** master task ************************************/
    if (taskid == MASTER) {
        printf("Matmul with %d MPI nodes.\n", mpiProcs);
        gettimeofday(&tInit, NULL);
        
        // Initialize arrays
        // printf("Initialize matrixes.\n");
        fillMatrix(ain, matrixSize, a);
        fillMatrix(bin, matrixSize, b);
        fillMatrix(cout, matrixSize, c);
        gettimeofday(&tFill, NULL);
        t = 1000 * (tFill.tv_sec - tInit.tv_sec) + (tFill.tv_usec - tInit.tv_usec) / 1000;
        printf("[TIME] Fill time %llu ms\n", t);
                
        // Send matrix data to the worker tasks
        printf("Send matrixes to workers.\n");
        int averow = matrixSize/mpiProcs;
        int extra = matrixSize%mpiProcs;
        int offset = 0;
        int mtype = FROM_MASTER;
        for (dest = 0; dest < mpiProcs; dest++) {
            rows = (dest < extra) ? averow+1 : averow;   	
            // printf("Sending %d rows to task %d offset=%d\n", rows, dest, offset);
            MPI_Isend(&offset, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD, &send_request);
            MPI_Isend(&rows, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD, &send_request);
            MPI_Isend(&a[offset*matrixSize], rows*matrixSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD, &send_request);
            MPI_Isend(&b, matrixSize*matrixSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD, &send_request);
            MPI_Isend(&c, rows*matrixSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD, &send_request);
            offset = offset + rows;
        }
        gettimeofday(&tSend, NULL);
        t = 1000 * (tSend.tv_sec - tFill.tv_sec) + (tSend.tv_usec - tFill.tv_usec) / 1000;
        printf("[TIME] Send time %llu ms\n", t);
    }
    
    /**************************** worker task ************************************/
    // Receive matrix
    printf("Receive IN matrixes on process %d.\n", taskid);
    gettimeofday(&tRecv, NULL);
    
    int offset = 0;
    int mtype = FROM_MASTER;
    MPI_Recv(&offset, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
    MPI_Recv(&rows, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
    MPI_Recv(&a, rows*matrixSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
    MPI_Recv(&b, matrixSize*matrixSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
    MPI_Recv(&c[offset*matrixSize], rows*matrixSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
    
    gettimeofday(&tRecvDone, NULL);
    t = 1000 * (tRecvDone.tv_sec - tRecv.tv_sec) + (tRecvDone.tv_usec - tRecv.tv_usec) / 1000;
    printf("[TIME] Recv time %llu ms in process %d\n", t, taskid);
    
    // Perform multiply accumulative
    // printf("Perform multiply accumulative on process %d.\n", taskid);
    for (i = 0; i < rows; i++) {
        for (k = 0; k < matrixSize; k++) {
            for (j = 0; j < matrixSize; j++) {
                c[i*matrixSize + j] = c[i*matrixSize + j] + a[i*matrixSize + k]*b[k*matrixSize + j];
            }
        }
    }
    
    gettimeofday(&tComp, NULL);
    t = 1000 * (tComp.tv_sec - tRecvDone.tv_sec) + (tComp.tv_usec - tRecvDone.tv_usec) / 1000;
    printf("[TIME] Multiplication time %llu ms in process %d\n", t, taskid);
    
    // Send back result to master
    // printf("Send result back to master on process %d.\n", taskid);
    mtype = FROM_WORKER;
    MPI_Isend(&offset, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &send_request);
    MPI_Isend(&rows, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &send_request);
    MPI_Isend(&c, rows*matrixSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &send_request);
    
    gettimeofday(&tSend, NULL);
    t = 1000 * (tSend.tv_sec - tComp.tv_sec) + (tSend.tv_usec - tComp.tv_usec) / 1000;
    printf("[TIME] Send time %llu ms in process %d\n", t, taskid);
    
    
    /**************************** master task ************************************/
    if (taskid == MASTER) {
        // Receive results from worker tasks
        printf("Receive results.\n");
        gettimeofday(&tRecvMaster, NULL);
        
        mtype = FROM_WORKER;
        for (i = 0; i < mpiProcs; i++) {
            MPI_Recv(&offset, 1, MPI_INT, i, mtype, MPI_COMM_WORLD, &status);
            MPI_Recv(&rows, 1, MPI_INT, i, mtype, MPI_COMM_WORLD, &status);
            MPI_Recv(&c[offset*matrixSize], rows*matrixSize, MPI_DOUBLE, i, mtype, MPI_COMM_WORLD, &status);
            // printf("  - Received results from task %d\n", i);
        }
        
        gettimeofday(&tRecvMasterDone, NULL);
        t = 1000 * (tRecvMasterDone.tv_sec - tRecvMaster.tv_sec) + (tRecvMasterDone.tv_usec - tRecvMaster.tv_usec) / 1000;
        printf("[TIME] Recv time %llu ms MASTER\n", t);
        
        // Print result
        //printMatrix(matrixSize, c);
        
        // Store result to file
        // printf("Store result matrix.\n");
        storeMatrix(cout, matrixSize, c);
        
        gettimeofday(&tStore, NULL);
        t = 1000 * (tStore.tv_sec - tRecvMasterDone.tv_sec) + (tStore.tv_usec - tRecvMasterDone.tv_usec) / 1000;
        printf("[TIME] Store time %llu ms\n", t);
        
        printf ("Done.\n");
        t = 1000 * (tStore.tv_sec - tInit.tv_sec) + (tStore.tv_usec - tInit.tv_usec) / 1000;
        printf("[TIME] TOTAL TIME %llu ms\n", t);
    }
    
    
    /**************************** Finalize ************************************/
    MPI_Finalize();
}
