package integratedtoolkit.nio.worker.util;

import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;

public interface ThreadBinder {
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException;
    public void releaseComputingUnits(int jobId);
}
