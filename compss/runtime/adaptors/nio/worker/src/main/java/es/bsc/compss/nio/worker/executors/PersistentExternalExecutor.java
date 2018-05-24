/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.exceptions.JobExecutionException;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.executors.util.BinaryInvoker;
import es.bsc.compss.nio.worker.executors.util.DecafInvoker;
import es.bsc.compss.nio.worker.executors.util.Invoker;
import es.bsc.compss.nio.worker.executors.util.MPIInvoker;
import es.bsc.compss.nio.worker.executors.util.OmpSsInvoker;
import es.bsc.compss.nio.worker.executors.util.OpenCLInvoker;
import es.bsc.compss.nio.worker.util.ExternalTaskStatus;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.nio.worker.util.TaskResultReader;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.RequestQueue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;


public abstract class PersistentExternalExecutor extends Executor {

    protected static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common" + File.separator
            + "lib";
    private static final String ERROR_UNSUPPORTED_JOB_TYPE = "Bindings don't support non-native tasks";

    // Storage properties
    // Storage Conf
    private static final boolean IS_STORAGE_ENABLED = System.getProperty(COMPSsConstants.STORAGE_CONF) != null
            && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("")
            && !System.getProperty(COMPSsConstants.STORAGE_CONF).equals("null");
    private static final String STORAGE_CONF = IS_STORAGE_ENABLED ? System.getProperty(COMPSsConstants.STORAGE_CONF) : "null";

    // Piper script properties
    public static final int MAX_RETRIES = 3;
    public static final String TOKEN_SEP = " ";
    public static final String TOKEN_NEW_LINE = "\n";
    public static final String END_TASK_TAG = "endTask";
    public static final String ERROR_TASK_TAG = "errorTask";
    public static final String QUIT_TAG = "quit";
    public static final String REMOVE_TAG = "remove";
    public static final String SERIALIZE_TAG = "serialize";
    private static final String EXECUTE_TASK_TAG = "task";
    
    static {
        System.loadLibrary("bindings_common");
    }

    public PersistentExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {

        super(nw, pool, queue);

        if (NIOTracer.isActivated()) {
            NIOTracer.disablePThreads();
        }
        

        if (NIOTracer.isActivated()) {
            NIOTracer.enablePThreads();
        }
    }

    public static native String executeInBinding(String args);
    
    public static native void initThread();
    
    public static native void finishThread();
    
    
    @Override
    public void setEnvironmentVariables(String hostnames, int numNodes, int cus, MethodResourceDescription reqs) {
        if (LOGGER.isDebugEnabled()) {
            System.out.println("HOSTNAMES: " + hostnames);
            System.out.println("NUM_NODES: " + numNodes);
            System.out.println("CPU_COMPUTING_UNITS: " + cus);
        }

        System.setProperty(Constants.COMPSS_HOSTNAMES, hostnames.toString());
        System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(numNodes));
        System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(cus));
    }

    @Override
    public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws Exception {

        // Check if it is a native method or not
        switch (nt.getMethodType()) {
            case METHOD:
                executeNativeMethod(nw, nt, outputsBasename, taskSandboxWorkingDir, assignedCoreUnits, assignedGPUs, assignedFPGAs);
                break;
            case BINARY:
                BinaryInvoker binaryInvoker = new BinaryInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                ExternalExecutor.executeNonNativeMethod(outputsBasename, binaryInvoker);
                break;
            case MPI:
                MPIInvoker mpiInvoker = new MPIInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                ExternalExecutor.executeNonNativeMethod(outputsBasename, mpiInvoker);
                break;
            case DECAF:
                DecafInvoker decafInvoker = new DecafInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                ExternalExecutor.executeNonNativeMethod(outputsBasename, decafInvoker);
                break;
            case OMPSS:
                OmpSsInvoker ompssInvoker = new OmpSsInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                ExternalExecutor.executeNonNativeMethod(outputsBasename, ompssInvoker);
                break;
            case OPENCL:
                OpenCLInvoker openclInvoker = new OpenCLInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                ExternalExecutor.executeNonNativeMethod(outputsBasename, openclInvoker);
                break;
        }
    }

    private void executeNativeMethod(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws JobExecutionException, SerializedObjectException {

        ArrayList<String> args = getTaskExecutionCommand(nw, nt, taskSandboxWorkingDir.getAbsolutePath(), assignedCoreUnits, assignedGPUs, assignedFPGAs);
        
        String externalCommand = ExternalExecutor.getExternalCommand(args, nt, nw, assignedCoreUnits, assignedGPUs);

        String command = outputsBasename + NIOWorker.SUFFIX_OUT + TOKEN_SEP + outputsBasename + NIOWorker.SUFFIX_ERR + TOKEN_SEP
                + externalCommand;

        executeExternal(nt.getJobId(), command, nt, nw);
        
    }

    @Override
    public void finish() {
        LOGGER.info("Finishing Persistent Executor");
        finishThread();
        LOGGER.info("End Finishing ExternalExecutor");
    }
    
    @Override
    public void start() {
        LOGGER.info("Starting Persistent Executor");
        initThread();
        LOGGER.info("Persistent Executor started");
    }

    public abstract ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs);


    
    private void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException {
        // Emit start task trace
        int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (NIOTracer.isActivated()) {
            emitStartTask(taskId, taskType);
        }

        String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;
        if (LOGGER.isDebugEnabled()) {
        	LOGGER.debug("Executing in binding: " + taskCMD);
        }
        
        String results = executeInBinding(taskCMD);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Result: " + results);
        }     
        ExternalTaskStatus taskStatus = new ExternalTaskStatus(results.split(" "));

        // Check task exit value
        Integer exitValue = taskStatus.getExitValue();
        if (exitValue != 0) {
            if (NIOTracer.isActivated()) {
                emitEndTask();
            }
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        }

        // Update parameters
        LOGGER.debug("Updating parameters for job " + jobId);
        for (int i = 0; i < taskStatus.getNumParameters(); ++i) {
            DataType paramType = taskStatus.getParameterType(i);
            if (paramType.equals(DataType.EXTERNAL_PSCO_T)) {
                String paramValue = taskStatus.getParameterValue(i);
                nt.getParams().get(i).setType(DataType.EXTERNAL_PSCO_T);
                nt.getParams().get(i).setValue(paramValue);
            }
        }

        // Emit end task trace
        if (NIOTracer.isActivated()) {
            emitEndTask();
        }
        LOGGER.debug("Job " + jobId + " has finished with exit value 0");
    }

    private void emitStartTask(int taskId, int taskType) {
        NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
        NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
    }

    private void emitEndTask() {
        NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
    }

}
