package integratedtoolkit.nio.worker.binders;

import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;


/**
 * Class for unbinded thread affinity
 *
 */
public class Unbinded implements ThreadBinder {

    /**
     * Creates a new thread binder without any binding
     */
    public Unbinded() {
    }

    @Override
    public int[] bindComputingUnits(int jobId, int numCUs) throws UnsufficientAvailableComputingUnitsException {
        return new int[] {};
    }

    @Override
    public void releaseComputingUnits(int jobId) {
    }

}
