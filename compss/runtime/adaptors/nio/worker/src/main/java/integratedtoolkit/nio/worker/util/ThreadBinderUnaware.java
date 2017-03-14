package integratedtoolkit.nio.worker.util;

import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


public class ThreadBinderUnaware implements ThreadBinder {

    private final int[] bindedComputingUnits;


    /**
     * Creates a new thread binder for unaware binds
     * 
     * @param numThreads
     */
    public ThreadBinderUnaware(int numThreads) {
        this.bindedComputingUnits = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            this.bindedComputingUnits[i] = -1;
        }
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException {
        if (numCUs == 0) {
            return new int[0];
        }
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

    @Override
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
