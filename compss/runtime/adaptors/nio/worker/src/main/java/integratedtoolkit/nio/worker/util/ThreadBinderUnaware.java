package integratedtoolkit.nio.worker.util;

import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;

public class ThreadBinderUnaware implements ThreadBinder {
    
    private int[] bindedComputingUnits;
    int numThreads;
    
    public ThreadBinderUnaware(){
        
    }
    
    public ThreadBinderUnaware(int numThreads) {
        this.numThreads = numThreads;
        this.bindedComputingUnits = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            this.bindedComputingUnits[i] = -1;
        }
    }
    
    /**
     * Bind numCUs core units to the job
     * 
     * @param jobId
     * @param numCUs
     * @return
     * @throws UnsufficientAvailableComputingUnitsException
     */
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException {
        int assignedCoreUnits[] = new int[numCUs];
        int numAssignedCores = 0;

        // Assign free CUs to the job
        synchronized (this.bindedComputingUnits) {
            for (int coreId = 0; coreId < this.bindedComputingUnits.length; ++coreId) {
                if (this.bindedComputingUnits[coreId] == -1) {
                    this.bindedComputingUnits[coreId] = jobId;
                    assignedCoreUnits[numAssignedCores] = coreId;
                    numAssignedCores++;
                }
                if (numAssignedCores == numCUs) {
                    break;
                }
            }
            
            // If the job doesn't have all the CUs it needs, it cannot run on occupied ones
            // Raise exception
            if (numAssignedCores != numCUs) {
                releaseComputingUnits(jobId);
                throw new UnsufficientAvailableComputingUnitsException("Not enough available computing units for task execution");
            }
        }
        return assignedCoreUnits;
    }

    /**
     * Release computing units occupied by the job
     * 
     * @param jobId
     */
    public void releaseComputingUnits(int jobId) {
        synchronized (bindedComputingUnits) {
            for (int coreId = 0; coreId < bindedComputingUnits.length; coreId++) {
                if (bindedComputingUnits[coreId] == jobId) {
                    bindedComputingUnits[coreId] = -1;
                }
            }
        }
    }
    
}
