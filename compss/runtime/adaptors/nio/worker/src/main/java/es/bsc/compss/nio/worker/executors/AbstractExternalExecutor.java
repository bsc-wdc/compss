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
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.executors.util.BinaryInvoker;
import es.bsc.compss.nio.worker.executors.util.DecafInvoker;
import es.bsc.compss.nio.worker.executors.util.Invoker;
import es.bsc.compss.nio.worker.executors.util.MPIInvoker;
import es.bsc.compss.nio.worker.executors.util.OmpSsInvoker;
import es.bsc.compss.nio.worker.executors.util.OpenCLInvoker;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.util.RequestQueue;

import java.io.File;
import java.util.ArrayList;


public abstract class AbstractExternalExecutor extends Executor {

    public static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common" + File.separator
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
    public static final String EXECUTE_TASK_TAG = "task";


    public AbstractExternalExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        super(nw, pool, queue);
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

    private static void executeNonNativeMethod(String outputsBasename, Invoker invoker) throws JobExecutionException {
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

    private void executeNativeMethod(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws JobExecutionException, SerializedObjectException {

        ArrayList<String> args = getTaskExecutionCommand(nw, nt, taskSandboxWorkingDir.getAbsolutePath(), assignedCoreUnits, assignedGPUs,
                assignedFPGAs);

        String externalCommand = getExternalCommand(args, nt, nw, assignedCoreUnits, assignedGPUs);

        String command = outputsBasename + NIOWorker.SUFFIX_OUT + TOKEN_SEP + outputsBasename + NIOWorker.SUFFIX_ERR + TOKEN_SEP
                + externalCommand;

        executeExternal(nt.getJobId(), command, nt, nw);
    }

    private static String getExternalCommand(ArrayList<String> args, NIOTask nt, NIOWorker nw, int[] assignedCoreUnits, int[] assignedGPUs)
            throws JobExecutionException, SerializedObjectException {
        addArguments(args, nt, nw);

        addThreadAffinity(args, assignedCoreUnits);

        addGPUAffinity(args, assignedGPUs);

        addHostlist(args);

        return getArgumentsAsString(args);

    }

    private static void addHostlist(ArrayList<String> args) {
        String hostlist = System.getProperty(Constants.COMPSS_HOSTNAMES);
        if (hostlist != null && !hostlist.isEmpty()) {
            args.add(hostlist);
        } else {
            args.add("-");
        }
    }

    private static void addThreadAffinity(ArrayList<String> args, int[] assignedCoreUnits) {
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

    private static void addGPUAffinity(ArrayList<String> args, int[] assignedGPUs) {
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

    private static String getArgumentsAsString(ArrayList<String> args) {
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
        lArgs.add(Integer.toString(nt.getNumReturns()));

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
                case EXTERNAL_PSCO_T:
                    lArgs.add(np.getValue().toString());
                    lArgs.add(np.isWriteFinalValue() ? "W" : "R");
                    break;
                case BINDING_OBJECT_T:
                    String extObjValue = np.getValue().toString();
                    LOGGER.debug("Generating command args for Binding_object " + extObjValue);
                    BindingObject bo = BindingObject.generate(extObjValue);
                    String originalData = "";
                    if (np.getData() != null) {
                        originalData = np.getData().getName();
                    } else {
                        LOGGER.debug("Data is null");
                    }
                    String destData = bo.getName();
                    if (!isRuntimeRenamed(destData)) {
                        // TODO: check if it happens also with binding_objects
                        // Corner case: destData is original name. Parameter is IN with shared disk, so
                        // destfile should be the same as the input.
                        destData = originalData;
                    }
                    lArgs.add(originalData + ":" + destData + ":" + np.isPreserveSourceData() + ":" + np.isWriteFinalValue() + ":"
                            + np.getOriginalName());
                    lArgs.add(Integer.toString(bo.getType()));
                    lArgs.add(Integer.toString(bo.getElements()));
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

    protected abstract void executeExternal(int jobId, String command, NIOTask nt, NIOWorker nw) throws JobExecutionException;

    protected abstract ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs);

    private static boolean isRuntimeRenamed(String filename) {
        return filename.startsWith("d") && filename.endsWith(".IT");
    }

    protected static void emitStartTask(int taskId, int taskType) {
        NIOTracer.emitEventAndCounters(taskType, NIOTracer.getTaskEventsType());
        NIOTracer.emitEvent(taskId, NIOTracer.getTaskSchedulingType());
    }

    protected static void emitEndTask() {
        NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskSchedulingType());
        NIOTracer.emitEventAndCounters(NIOTracer.EVENT_END, NIOTracer.getTaskEventsType());
    }

}
