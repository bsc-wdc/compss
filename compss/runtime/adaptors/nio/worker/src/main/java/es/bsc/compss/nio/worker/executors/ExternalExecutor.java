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


public abstract class ExternalExecutor extends Executor {

    protected static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common" + File.separator
            + "lib";

    private static final String ERROR_PIPE_CLOSE = "Error on closing pipe ";
    private static final String ERROR_PIPE_QUIT = "Error sending quit to pipe ";
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

    private final String writePipe; // Pipe for sending executions
    private final TaskResultReader taskResultReader; // Process result reader (initialized by PoolManager,
                                                     // started/stopped by us)


    public ExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue, String writePipe,
            TaskResultReader resultReader) {

        super(nw, pool, queue);

        this.writePipe = writePipe;
        this.taskResultReader = resultReader;

        if (NIOTracer.isActivated()) {
            NIOTracer.disablePThreads();
        }
        // Start task Reader
        this.taskResultReader.start();

        if (NIOTracer.isActivated()) {
            NIOTracer.enablePThreads();
        }
    }

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
                executeNonNativeMethod(outputsBasename, binaryInvoker);
                break;
            case MPI:
                MPIInvoker mpiInvoker = new MPIInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, mpiInvoker);
                break;
            case DECAF:
                DecafInvoker decafInvoker = new DecafInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, decafInvoker);
                break;
            case OMPSS:
                OmpSsInvoker ompssInvoker = new OmpSsInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, ompssInvoker);
                break;
            case OPENCL:
                OpenCLInvoker openclInvoker = new OpenCLInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, openclInvoker);
                break;
        }
    }

    private void executeNativeMethod(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws JobExecutionException, SerializedObjectException {

        ArrayList<String> args = getTaskExecutionCommand(nw, nt, taskSandboxWorkingDir.getAbsolutePath(), assignedCoreUnits, assignedGPUs, assignedFPGAs);
        addArguments(args, nt, nw);

        addThreadAffinity(args, assignedCoreUnits);
        
        addGPUAffinity(args, assignedGPUs);
        
        addHostlist(args);

        String externalCommand = getArgumentsAsString(args);

        String command = outputsBasename + NIOWorker.SUFFIX_OUT + TOKEN_SEP + outputsBasename + NIOWorker.SUFFIX_ERR + TOKEN_SEP
                + externalCommand;

        executeExternal(nt.getJobId(), command, nt, nw);
    }

    private void addHostlist(ArrayList<String> args) {
		String hostlist = System.getProperty(Constants.COMPSS_HOSTNAMES);
    	if (hostlist!=null && !hostlist.isEmpty()){
    		args.add(hostlist);
    	}else{
    		args.add("-");
    	}
	}

	private void addThreadAffinity(ArrayList<String> args, int[] assignedCoreUnits) {
        String computingUnits;
        if (assignedCoreUnits.length == 0) {
            computingUnits = "-";
        } else {
            computingUnits = String.valueOf(assignedCoreUnits[0]);
            for (int i = 1; i < assignedCoreUnits.length; ++i) {
                computingUnits = computingUnits + "," + assignedCoreUnits[i];
            }
        }
        args.add(computingUnits);
    }
    
    private void addGPUAffinity(ArrayList<String> args, int[] assignedGPUs) {
        String computingUnits;
        if (assignedGPUs.length == 0) {
            computingUnits = "-";
        } else {
            computingUnits = String.valueOf(assignedGPUs[0]);
            for (int i = 1; i < assignedGPUs.length; ++i) {
                computingUnits = computingUnits + "," + assignedGPUs[i];
            }
        }
        args.add(computingUnits);
    }

    private void executeNonNativeMethod(String outputsBasename, Invoker invoker) throws JobExecutionException {
        /* Register outputs **************************************** */
        NIOWorker.registerOutputs(outputsBasename);

        /* TRY TO PROCESS THE TASK ******************************** */
        System.out.println("[EXTERNAL EXECUTOR] executeNonNativeTask - Begin task execution");
        try {
            invoker.processTask();
            invoker.serializeBinaryExitValue();
        } catch (JobExecutionException jee) {
            System.err.println("[EXTERNAL EXECUTOR] executeNonNativeTask - Error in task execution");
            jee.printStackTrace();
            throw jee;
        } finally {
            System.out.println("[EXTERNAL EXECUTOR] executeNonNativeTask - End task execution");
            /* Unregister outputs **************************************** */
            NIOWorker.unregisterOutputs();
        }
    }

    @Override
    public void finish() {
        LOGGER.info("Finishing ExternalExecutor");

        // Send quit tag to pipe
        LOGGER.debug("Send quit tag to pipe " + writePipe);
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            FileOutputStream output = null;
            try {
                output = new FileOutputStream(writePipe, true);
                String quitCMD = QUIT_TAG + TOKEN_NEW_LINE;
                output.write(quitCMD.getBytes());
                output.flush();
            } catch (Exception e) {
                LOGGER.warn("Error on writing on pipe " + writePipe + ". Retrying " + retries + "/" + MAX_RETRIES);
                ++retries;
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception e) {
                        ErrorManager.error(ERROR_PIPE_CLOSE + writePipe, e);
                    }
                }
            }
            done = true;
        }
        if (!done) {
            ErrorManager.error(ERROR_PIPE_QUIT + writePipe);
        }

        // ------------------------------------------------------
        // Ask TaskResultReader to stop and wait for it to finish
        LOGGER.debug("Waiting for TaskResultReader");
        Semaphore sem = new Semaphore(0);
        taskResultReader.shutdown(sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        LOGGER.info("End Finishing ExternalExecutor");
    }

    public abstract ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs);

    private String getArgumentsAsString(ArrayList<String> args) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String c : args) {
            if (!first) {
                sb.append(" ");
            } else {
                first = false;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static void addArguments(ArrayList<String> lArgs, NIOTask nt, NIOWorker nw)
            throws JobExecutionException, SerializedObjectException {

        lArgs.add(Boolean.toString(NIOTracer.isActivated()));
        lArgs.add(Integer.toString(nt.getTaskId()));
        lArgs.add(Boolean.toString(nt.isWorkerDebug()));
        lArgs.add(STORAGE_CONF);

        // The implementation to execute externally can only be METHOD but we double check it
        if (nt.getMethodType() != MethodType.METHOD) {
            throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }

        // Add method classname and methodname
        MethodImplementation impl = (MethodImplementation) nt.getMethodImplementation();
        lArgs.add(String.valueOf(impl.getMethodType()));
        lArgs.add(impl.getDeclaringClass());
        lArgs.add(impl.getAlternativeMethodName());

        // Slave nodes and cus description
        lArgs.add(String.valueOf(nt.getSlaveWorkersNodeNames().size()));
        lArgs.addAll(nt.getSlaveWorkersNodeNames());
        lArgs.add(String.valueOf(nt.getResourceDescription().getTotalCPUComputingUnits()));

        // Add target
        lArgs.add(Boolean.toString(nt.hasTarget()));

        // Add return type
        if (nt.hasReturn()) {
            DataType returnType = nt.getParams().getLast().getType();
            lArgs.add(Integer.toString(returnType.ordinal()));
        } else {
            lArgs.add("null");
        }

        // Add parameters
        lArgs.add(Integer.toString(nt.getNumParams()));
        for (NIOParam np : nt.getParams()) {
            DataType type = np.getType();
            lArgs.add(Integer.toString(type.ordinal()));
            lArgs.add(Integer.toString(np.getStream().ordinal()));
            lArgs.add(np.getPrefix());
            switch (type) {
                case FILE_T:
                    // Passing originalName link instead of renamed file

                    String originalFile = "";
                    if (np.getData() != null) {
                        originalFile = np.getData().getName();

                    }
                    String destFile = new File(np.getValue().toString()).getName();
                    if (!isRuntimeRenamed(destFile)) {
                        // Treat corner case: Destfile is original name. Parameter is INPUT with shared disk, so
                        // destfile should be the same as the input.
                        destFile = originalFile;
                    }
                    lArgs.add(originalFile + ":" + destFile + ":" + np.isPreserveSourceData() + ":" + np.isWriteFinalValue() + ":"
                            + np.getOriginalName());
                    break;
                case OBJECT_T:
                case PSCO_T:
                case EXTERNAL_OBJECT_T:
                    lArgs.add(np.getValue().toString());
                    lArgs.add(np.isWriteFinalValue() ? "W" : "R");
                    break;
                case STRING_T:
                    String value = np.getValue().toString();
                    String[] vals = value.split(" ");
                    int numSubStrings = vals.length;
                    lArgs.add(Integer.toString(numSubStrings));
                    for (String v : vals) {
                        lArgs.add(v);
                    }
                    break;
                default:
                    lArgs.add(np.getValue().toString());
            }
        }
    }

    private static boolean isRuntimeRenamed(String filename) {
        return filename.startsWith("d") && filename.endsWith(".IT");
    }

    private void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException {
        // Emit start task trace
        int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (NIOTracer.isActivated()) {
            emitStartTask(taskId, taskType);
        }

        LOGGER.debug("Starting job process ...");
        // Send executeTask tag to pipe
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            // Send to pipe : task tID command(jobOut jobErr externalCMD) \n
            String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("EXECUTOR COMMAND: " + taskCMD);
            }

            try (FileOutputStream output = new FileOutputStream(writePipe, true);) {
                output.write(taskCMD.getBytes());
                output.flush();
                output.close();
                done = true;
            } catch (Exception e) {
                LOGGER.debug("Error on pipe write. Retry");
                ++retries;
            }
        }

        if (!done) {
            if (NIOTracer.isActivated()) {
                emitEndTask();
            }
            LOGGER.error("ERROR: Could not execute job " + jobId + " because cannot write in pipe");
            throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
        }

        // Retrieving job result
        LOGGER.debug("Waiting for job " + jobId + " completion");
        Semaphore sem = new Semaphore(0);
        taskResultReader.askForTaskEnd(jobId, sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.debug("Job " + jobId + " completed. Retrieving task result");
        ExternalTaskStatus taskStatus = taskResultReader.getTaskStatus(jobId);

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
            if (paramType.equals(DataType.EXTERNAL_OBJECT_T)) {
                String paramValue = taskStatus.getParameterValue(i);
                nt.getParams().get(i).setType(DataType.EXTERNAL_OBJECT_T);
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
