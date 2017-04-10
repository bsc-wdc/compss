#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define MASTER 0               /* taskid of first task */
#define FROM_MASTER 1          /* setting a message type */
#define FROM_WORKER 2          /* setting a message type */

/*
 * THE MATRIXES MUST BE SQUARED
 * THE MATRIX SIZE MUST BE A POWER OF TWO: 64, 128, 256, 512, 1024, 2048, 4096
 * THE NUMBER OF PROCESSES MUST BE 2, 5 OR 17 (MASTER + 1, 4 OR 16 WORKERS)
 */

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
            //printf("%lf\n", mat[i*matrixSize + j]);
        }
        fprintf(file, "\n");
    }
    fclose(file);
}

void printMatrix(int matrixSize, const double* mat) {
    int i, j;
    
    printf("******************************************************\n");
    printf("Result Matrix:\n");
    for (i = 0; i < matrixSize; i++) {
        printf("\n"); 
        for (j = 0; j < matrixSize; j++) {
            printf("%6.2f   ", mat[i*matrixSize + j]);
        }
    }
    printf("\n******************************************************\n");
}

int main (int argc, char *argv[]) {   
    if (argc != 5) {
        printf("Incorrect usage: matmul <matrixSize> <Ain> <Bin> <Cout>. Quitting...\n");
        exit(1);
    }
    
    /**************************** Initialize ************************************/
    int matrixSize = atoi(argv[1]);     // Number of rows/columns in matrix A B and C
    char* ain = argv[2];                // FileName of Ain
    char* bin = argv[3];                // FileName of Bin
    char* cout = argv[4];               // FileName of Cout
    
    double a[matrixSize*matrixSize];    // Matrix A to be multiplied
    double b[matrixSize*matrixSize];    // Matrix B to be multiplied
    double c[matrixSize*matrixSize];    // Result matrix C    
    
    // MPI configuration
    int numProcs;                       // Number of MPI Nodes
    int numProcsPerDimension;           // Number of blocks per row/column
    int blockSize;                      // Block size
    
    int taskId;                         // Task identifier
    MPI_Status status;                  // Status variable for MPI communications
    int mtype;                          // Message type
    
    // Misc variables
    int i, j, k, dest, row, workerRow, workerColumn;

    /************************ Initialize MPI ENV ********************************/
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &taskId);
    MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
    numProcsPerDimension = sqrt(numProcs);
    blockSize = matrixSize / numProcsPerDimension;    
    
    /**************************** master send ************************************/
    if (taskId == MASTER) {
        printf("Matmul with %d MPI nodes.\n", numProcs);
        
        // Initialize arrays
        printf("Initialize matrixes.\n");
        fillMatrix(ain, matrixSize, a);
        fillMatrix(bin, matrixSize, b);
        fillMatrix(cout, matrixSize, c);
                
        // Send matrix data to the worker tasks
        printf("Send data\n");
        mtype = FROM_MASTER;
        for (dest = 1; dest < numProcs; dest++) {
            workerRow = (dest/numProcsPerDimension)*blockSize;
            workerColumn = (dest%numProcsPerDimension)*blockSize;
            
            // Send block parameters
            MPI_Send(&workerRow, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
            MPI_Send(&workerColumn, 1, MPI_INT, dest, mtype, MPI_COMM_WORLD);
            
            // Send block rows of A
            MPI_Send(&a[workerRow*matrixSize], matrixSize*blockSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
            
            // Send block columns of B
            for (row = 0; row < matrixSize; ++row) {
                MPI_Send(&b[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
            }
            
            // Send block of C
            for (row = workerRow; row < workerRow + blockSize; ++row) {
                MPI_Send(&c[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD);
            }
        }
    }
    
    /**************************** worker task ************************************/
    if (taskId > MASTER) {
        // Receive matrix
        printf("Receive IN matrixes on process %d.\n", taskId);
        mtype = FROM_MASTER;
        MPI_Recv(&workerRow, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
        MPI_Recv(&workerColumn, 1, MPI_INT, MASTER, mtype, MPI_COMM_WORLD, &status);
        
        MPI_Recv(&a[workerRow*matrixSize], matrixSize*blockSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
        
        for (row = 0; row < matrixSize; ++row) {
            MPI_Recv(&b[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
        }
        
        for (row = workerRow; row < workerRow + blockSize; ++row) {
            MPI_Recv(&c[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD, &status);
        }
    } else {
        workerRow = 0;
        workerColumn = 0;
    }
    
    // Perform multiply accumulative
    printf("Perform multiply accumulative on process %d.\n", taskId);    
    for (i = 0; i < blockSize; ++i) {
        for (k = 0; k < matrixSize; ++k) {
            for (j = 0; j < blockSize; ++j) {
                c[(workerRow + i)*matrixSize + workerColumn + j] = c[(workerRow + i)*matrixSize + workerColumn + j] + 
                    a[(workerRow + i)*matrixSize + k]*b[k*matrixSize + workerColumn + j];
            }
        }
    }
    
    // Send back result to master
    printf("Send result back to master on process %d.\n", taskId);
    mtype = FROM_WORKER;
    for (row = workerRow; row < workerRow + blockSize; ++row) {
        MPI_Send(&c[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, MASTER, mtype, MPI_COMM_WORLD);
    }
    
    /**************************** master receive ************************************/
    if (taskId == MASTER) {
        // Receive results from worker tasks
        printf("Receive results.\n");
        mtype = FROM_WORKER;
        for (dest = 0; dest < numProcs; dest++) {
            workerRow = (dest/numProcsPerDimension)*blockSize;
            workerColumn = (dest%numProcsPerDimension)*blockSize;
            
            // Receive C block
            for (row = workerRow; row < workerRow + blockSize; ++row) {
                MPI_Recv(&c[row*matrixSize + workerColumn], blockSize, MPI_DOUBLE, dest, mtype, MPI_COMM_WORLD, &status);
            }
            printf("  - Received results from task %d\n", dest);
        }
        
        // Print result
        // printMatrix(matrixSize, c);
        
        // Store result to file
        // printf("Store result matrix.\n");
        storeMatrix(cout, matrixSize, c);
        
        printf ("Done.\n");
    }
    
    /**************************** Finalize ************************************/
    MPI_Finalize();
}
