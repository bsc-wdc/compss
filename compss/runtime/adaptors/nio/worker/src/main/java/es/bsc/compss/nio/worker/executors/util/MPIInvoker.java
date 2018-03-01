package es.bsc.compss.nio.worker.executors.util;

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.worker.invokers.GenericInvoker;


public class MPIInvoker extends Invoker {

    private static final String ERROR_MPI_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_MPI_BINARY = "ERROR: Invalid mpiBinary";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private final String mpiBinary;


    public MPIInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        MPIImplementation mpiImpl = null;
        try {
            mpiImpl = (MPIImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.mpiRunner = mpiImpl.getMpiRunner();
        this.mpiBinary = mpiImpl.getBinary();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        checkArguments();
        return invokeMPIMethod();
    }

    private void checkArguments() throws JobExecutionException {
        if (this.mpiRunner == null || this.mpiRunner.isEmpty()) {
            throw new JobExecutionException(ERROR_MPI_RUNNER);
        }
        if (this.mpiBinary == null || this.mpiBinary.isEmpty()) {
            throw new JobExecutionException(ERROR_MPI_BINARY);
        }
        if (this.target.getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object invokeMPIMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.mpiBinary + " in " + this.nw.getHostName());
        try {
            return GenericInvoker.invokeMPIMethod(this.mpiRunner, this.mpiBinary, this.values, this.streams, this.prefixes,
                    this.taskSandboxWorkingDir);
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

}
