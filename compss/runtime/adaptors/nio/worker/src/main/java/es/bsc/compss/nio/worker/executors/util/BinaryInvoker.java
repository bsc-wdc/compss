package es.bsc.compss.nio.worker.executors.util;

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;

import es.bsc.compss.types.implementations.BinaryImplementation;

import es.bsc.compss.worker.invokers.GenericInvoker;


public class BinaryInvoker extends Invoker {

    private final String binary;


    public BinaryInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        BinaryImplementation binaryImpl = null;
        try {
            binaryImpl = (BinaryImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.binary = binaryImpl.getBinary();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.binary + " in " + this.nw.getHostName());
        try {
            return GenericInvoker.invokeBinaryMethod(this.binary, this.values, this.streams, this.prefixes, this.taskSandboxWorkingDir);
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

}
