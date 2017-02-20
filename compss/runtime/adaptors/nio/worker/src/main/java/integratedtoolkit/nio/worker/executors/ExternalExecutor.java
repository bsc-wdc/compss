package integratedtoolkit.nio.worker.executors;

import integratedtoolkit.ITConstants;

import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.executors.util.BinaryInvoker;
import integratedtoolkit.nio.worker.executors.util.Invoker;
import integratedtoolkit.nio.worker.executors.util.MPIInvoker;
import integratedtoolkit.nio.worker.executors.util.OmpSsInvoker;
import integratedtoolkit.nio.worker.executors.util.OpenCLInvoker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.TaskResultReader;

import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.annotations.parameter.DataType;

import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.RequestQueue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;


public abstract class ExternalExecutor extends Executor {

    private static final String ERROR_PIPE_CLOSE = "Error on closing pipe ";
    private static final String ERROR_PIPE_QUIT = "Error sending quit to pipe ";
    private static final String ERROR_UNSUPPORTED_JOB_TYPE = "Bindings don't support non-native tasks";
    private static final String ERROR_SERIALIZED_OBJ = "ERROR: Cannot obtain object";

    // Storage properties
    // Storage Conf
    private static final boolean IS_STORAGE_ENABLED = System.getProperty(ITConstants.IT_STORAGE_CONF) != null
            && !System.getProperty(ITConstants.IT_STORAGE_CONF).equals("")
            && !System.getProperty(ITConstants.IT_STORAGE_CONF).equals("null");
    private static final String STORAGE_CONF = IS_STORAGE_ENABLED ? System.getProperty(ITConstants.IT_STORAGE_CONF) : "null";

    // Piper script properties
    public static final int MAX_RETRIES = 3;
    public static final String TOKEN_SEP = " ";
    public static final String TOKEN_NEW_LINE = "\n";
    public static final String END_TASK_TAG = "endTask";
    public static final String QUIT_TAG = "quit";
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
        // TODO: Add environment variables for MPI or Ompss tasks executed with bindings
    }

    @Override
    public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs) throws Exception {
        // Check if it is a native method or not
        switch (nt.getMethodType()) {
            case METHOD:
                executeNativeMethod(nw, nt, outputsBasename, taskSandboxWorkingDir, assignedCoreUnits, assignedGPUs);
                break;
            case BINARY:
                BinaryInvoker binaryInvoker = new BinaryInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, binaryInvoker);
                break;
            case MPI:
                MPIInvoker mpiInvoker = new MPIInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                executeNonNativeMethod(outputsBasename, mpiInvoker);
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
            int[] assignedGPUs) throws JobExecutionException, SerializedObjectException {

        ArrayList<String> args = getTaskExecutionCommand(nw, nt, taskSandboxWorkingDir.getAbsolutePath(), assignedCoreUnits, assignedGPUs);
        addArguments(args, nt, nw);
        String externalCommand = getArgumentsAsString(args);

        String command = outputsBasename + NIOWorker.SUFFIX_OUT + TOKEN_SEP + outputsBasename + NIOWorker.SUFFIX_ERR + TOKEN_SEP
                + externalCommand;

        executeExternal(nt.getJobId(), command, nt, nw);
    }

    private void executeNonNativeMethod(String outputsBasename, Invoker invoker) throws JobExecutionException {
        /* Register outputs **************************************** */
        NIOWorker.registerOutputs(outputsBasename);

        /* TRY TO PROCESS THE TASK ******************************** */
        System.out.println("[EXTERNAL EXECUTOR] executeNonNativeTask - Begin task execution");
        try {
            invoker.processTask();
        } catch (JobExecutionException jee) {
            System.out.println("[EXTERNAL EXECUTOR] executeNonNativeTask - Error in task execution");
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
        logger.info("Finishing ExternalExecutor");

        // Send quit tag to pipe
        logger.debug("Send quit tag to pipe " + writePipe);
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
                logger.warn("Error on writing on pipe " + writePipe + ". Retrying " + retries + "/" + MAX_RETRIES);
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
        logger.debug("Waiting for TaskResultReader");
        Semaphore sem = new Semaphore(0);
        taskResultReader.shutdown(sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("End Finishing ExternalExecutor");
    }

    public abstract ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs);

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
        lArgs.add(Boolean.toString(nt.isHasTarget()));

        // Add return type
        if (nt.isHasReturn()) {
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
                    lArgs.add(np.getOriginalName());
                    break;
                case OBJECT_T:
                case PSCO_T:
                    lArgs.add(np.getValue().toString());
                    lArgs.add(np.isWriteFinalValue() ? "W" : "R");
                    break;
                case EXTERNAL_PSCO_T:
                    String pscoId = null;
                    try {
                        pscoId = (String) nw.getObject(np.getValue().toString());
                    } catch (SerializedObjectException soe) {
                        throw new JobExecutionException(ERROR_SERIALIZED_OBJ, soe);
                    }
                    lArgs.add(pscoId);
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

    private void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException {
        // Emit start task trace
        int taskType = nt.getTaskType() + 1; // +1 Because Task ID can't be 0 (0 signals end task)
        int taskId = nt.getTaskId();

        if (NIOTracer.isActivated()) {
            emitStartTask(taskId, taskType);
        }

        logger.debug("Starting job process ...");
        // Send executeTask tag to pipe
        boolean done = false;
        int retries = 0;
        while (!done && retries < MAX_RETRIES) {
            FileOutputStream output = null;
            try {
                // Send to pipe : task tID command(jobOut jobErr externalCMD) \n
                String taskCMD = EXECUTE_TASK_TAG + TOKEN_SEP + jobId + TOKEN_SEP + command + TOKEN_NEW_LINE;

                if (logger.isDebugEnabled()) {
                    logger.debug("EXECUTOR COMMAND: " + taskCMD);
                }

                output = new FileOutputStream(writePipe, true);
                output.write(taskCMD.getBytes());
                output.flush();
            } catch (Exception e) {
                logger.debug("Error on pipe write. Retry");
                ++retries;
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception e) {
                        if (NIOTracer.isActivated()) {
                            emitEndTask(taskId);
                        }
                        throw new JobExecutionException("Job " + jobId + " has failed. Cannot close pipe");
                    }
                }
            }
            done = true;
        }
        if (!done) {
            if (NIOTracer.isActivated()) {
                emitEndTask(taskId);
            }
            throw new JobExecutionException("Job " + jobId + " has failed. Cannot write in pipe");
        }

        // Retrieving job result
        Semaphore sem = new Semaphore(0);
        taskResultReader.askForTaskEnd(jobId, sem);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int exitValue = taskResultReader.getExitValue(jobId);

        // Emit end task trace
        if (NIOTracer.isActivated()) {
            emitEndTask(taskId);
        }

        logger.debug("Task finished");
        if (exitValue != 0) {
            throw new JobExecutionException("Job " + jobId + " has failed. Exit values is " + exitValue);
        } else {
            logger.debug("Job " + jobId + " has finished with exit value 0");
        }
    }

    private void emitStartTask(int taskId, int taskType) {
        NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
        NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEvent(taskId, NIOTracer.getSyncType());
        // NIOTracer.emitEvent(NIOTracer.Event.PROCESS_CREATION.getId(), NIOTracer.Event.PROCESS_CREATION.getType());
    }

    private void emitEndTask(int taskId) {
        NIOTracer.emitEvent(taskId, NIOTracer.getSyncType());
        // NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.PROCESS_DESTRUCTION.getType());
        NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
    }

}
