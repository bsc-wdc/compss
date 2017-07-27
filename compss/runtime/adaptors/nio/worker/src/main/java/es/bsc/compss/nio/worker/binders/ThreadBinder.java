package es.bsc.compss.nio.worker.binders;

import es.bsc.compss.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


public interface ThreadBinder {

    /**
     * Binds @numCUs computing units of a @jobId
     * 
     * @param jobId
     * @param numCUs
     * @return
     * @throws UnsufficientAvailableComputingUnitsException
     */
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException;

    /**
     * Releases the computing units previously requested by the job @jobId
     * 
     * @param jobId
     */
    public void releaseComputingUnits(int jobId);

}
