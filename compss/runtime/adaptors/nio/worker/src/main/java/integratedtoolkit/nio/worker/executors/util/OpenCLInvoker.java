package integratedtoolkit.nio.worker.executors.util;

import java.io.File;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.types.implementations.OpenCLImplementation;


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
