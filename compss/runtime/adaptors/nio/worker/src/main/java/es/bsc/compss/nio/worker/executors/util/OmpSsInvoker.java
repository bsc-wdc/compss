package es.bsc.compss.nio.worker.executors.util;

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.worker.invokers.GenericInvoker;


public class OmpSsInvoker extends Invoker {

    private final String ompssBinary;


    public OmpSsInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        OmpSsImplementation ompssImpl = null;
        try {
            ompssImpl = (OmpSsImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.ompssBinary = ompssImpl.getBinary();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + ompssBinary + " in " + nw.getHostName());
        try {
            return GenericInvoker.invokeOmpSsMethod(this.ompssBinary, this.values, this.streams, this.prefixes, this.taskSandboxWorkingDir);
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

}
