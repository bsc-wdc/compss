package es.bsc.compss.nio.worker.executors.util;

import java.io.File;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.implementations.OpenCLImplementation;


public class OpenCLInvoker extends Invoker {

    private final String kernel;


    public OpenCLInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        OpenCLImplementation openclImpl = null;
        try {
            openclImpl = (OpenCLImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.kernel = openclImpl.getKernel();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        // TODO: Handle OpenCL invoke
        throw new JobExecutionException("Unsupported Method Type OPENCL with kernel" + this.kernel);
    }

}
