package integratedtoolkit.nio.worker.executors.util;

import java.io.File;

import integratedtoolkit.exceptions.InvokeExecutionException;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.implementations.DecafImplementation;
import integratedtoolkit.types.implementations.MPIImplementation;
import integratedtoolkit.worker.invokers.GenericInvoker;


public class DecafInvoker extends Invoker {

    private static final String ERROR_DECAF_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_DECAF_BINARY = "ERROR: Invalid wfScript";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private String dfScript;
    private String dfExecutor;
    private String dfLib;


    public DecafInvoker(NIOWorker nw, NIOTask nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        DecafImplementation decafImpl = null;
        try {
            decafImpl = (DecafImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.mpiRunner = decafImpl.getMpiRunner();
        this.dfScript = decafImpl.getDfScript();
        this.dfExecutor = decafImpl.getDfExecutor();
        this.dfLib = decafImpl.getDfLib();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        checkArguments();
        return invokeMPIMethod();
    }

    private void checkArguments() throws JobExecutionException {
        if (this.mpiRunner == null || this.mpiRunner.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_RUNNER);
        }
        if (this.dfScript == null || this.dfScript.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_BINARY);
        }
        if (!this.dfScript.startsWith(File.separator)){
        	this.dfScript=nw.getAppDir()+File.separator+this.dfScript;
        }
        if (this.dfExecutor == null || this.dfExecutor.isEmpty() ||  this.dfExecutor.equals(Constants.UNASSIGNED)) {
        	this.dfExecutor = "executor.sh";
        }
        if (!this.dfExecutor.startsWith(File.separator) && !this.dfExecutor.startsWith("./")){
        	this.dfExecutor = "./"+this.dfExecutor;
        }
        if (this.dfLib == null || this.dfLib.isEmpty()) {
        	this.dfLib = "null";
        }
        if (this.target.getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object invokeMPIMethod() throws JobExecutionException {
        logger.info("Invoked " + this.dfScript + " in " + this.nw.getHostName());
        try {
            return GenericInvoker.invokeDecafMethod(nw.getInstallDir()+DecafImplementation.SCRIPT_PATH, this.dfScript, this.dfExecutor, this.dfLib, this.mpiRunner, this.values, this.hasReturn, this.streams, this.prefixes,
                    this.taskSandboxWorkingDir);
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

}
