/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers;

import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.impl.DoNothingApplicationMonitor;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.InvocationRunner;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.ExecType;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.compss.worker.COMPSsWorker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Invoker extends DoNothingApplicationMonitor {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    protected static final String ERROR_METHOD_DEFINITION = "Incorrect method definition for task of type ";
    protected static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
    protected static final String ERROR_UNKNOWN_TYPE = "ERROR: Unrecognised type ";

    public static final String COMPSS_NUM_NODES = "COMPSS_NUM_NODES";
    public static final String COMPSS_NODES = "COMPSS_NODES";
    public static final String COMPSS_HOSTNAMES = "COMPSS_HOSTNAMES";
    public static final String COMPSS_NUM_PROCS = "COMPSS_NUM_PROCS";
    public static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";
    public static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";
    public static final String COMPSS_BINDED_GPUS = "COMPSS_BINDED_GPUS";
    public static final String COMPSS_BINDED_CPUS = "COMPSS_BINDED_CPUS";
    public static final String IB_SUFFIX = "-ib0";

    protected final InvocationContext context;
    protected final Invocation invocation;
    protected InvocationRunner runner;

    protected final ExecutionSandbox sandBox;
    protected final InvocationResources assignedResources;

    protected final int computingUnits;
    protected final String workers;
    protected final int numWorkers;
    protected final List<String> hostnames;


    /**
     * Invoker constructor.
     *
     * @param context Invocation context
     * @param invocation task execution invocation description (job)
     * @param sandbox task execution sandboxed working dir
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating task execution (job)
     */
    public Invoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        this.context = context;
        this.invocation = invocation;
        this.sandBox = sandbox;
        this.assignedResources = assignedResources;

        /* Parse execution infrastructure **************************************** */
        // ComputingUnits flags
        ResourceDescription rd = this.invocation.getRequirements();
        if (this.invocation.getTaskType() == TaskType.METHOD) {
            boolean mpiImpl = false;
            AbstractMethodImplementation impl = this.invocation.getMethodImplementation();

            if (impl.getMethodType() == MethodType.PYTHON_MPI || impl.getMethodType() == MethodType.MPI) {
                mpiImpl = true;
            }

            if (impl.isIO() && mpiImpl) {
                this.computingUnits = ((MethodResourceDescription) rd).getTotalMPIComputingUnits();
            } else {
                this.computingUnits = ((MethodResourceDescription) rd).getTotalCPUComputingUnits();
            }
        } else {
            this.computingUnits = 0;
        }

        // Multi-Node flags
        this.hostnames = invocation.getSlaveNodesNames();
        hostnames.add(context.getHostName());
        this.numWorkers = hostnames.size();
        this.workers = buildWorkersString(hostnames, computingUnits);

        /* Parse the parameters ************************************ */
        AbstractMethodImplementation impl = invocation.getMethodImplementation();
        for (InvocationParam np : invocation.getParams()) {
            processParameter(np);
            // Check if object is still null
            /*
             * if (np.getValue() == null) { StringBuilder sb = new StringBuilder();
             * sb.append("Object parameter ").append(paramIdx); sb.append(" with renaming ").append(np.getDataMgmtId());
             * sb.append(" in MethodDefinition ").append(impl.getMethodDefinition());
             * sb.append(" is null!").append("\n");
             * 
             * throw new JobExecutionException(sb.toString()); }
             */
        }
        if (invocation.getTarget() != null) {
            processParameter(invocation.getTarget());
        }

        /* DEBUG information *************************************** */
        if (invocation.isDebugEnabled()) {
            // Print request information
            PrintStream out = context.getThreadOutStream();
            out.println("WORKER - Parameters of execution:");
            out.println("  * Method type: " + impl.getMethodType());
            out.println("  * Method definition: " + impl.getMethodDefinition());
            out.print("  * Parameter types:");
            for (InvocationParam p : invocation.getParams()) {
                if (p.getValueClass() != null) {
                    out.print(" " + p.getValueClass().getName());
                }
            }
            out.println("");

            out.print("  * Parameter names:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getName());
            }
            out.println("");

            out.print("  * Parameter values:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getValue());
            }
            out.println("");

            out.print("  * Parameter streams:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getStdIOStream());
            }
            if (invocation.getTarget() != null) {
                out.print(" " + invocation.getTarget().getStdIOStream());
            }
            out.println("");

            out.print("  * Parameter prefixes:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getPrefix());
            }
            if (invocation.getTarget() != null) {
                out.print(" " + invocation.getTarget().getPrefix());
            }
            out.println("");

            out.println("  * Has Target: " + (invocation.getTarget() != null));
            out.println("  * Has Return: " + (!invocation.getResults().isEmpty()));
        }
    }

    private static String buildWorkersString(List<String> hostnames, int computingUnits) {
        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (String hostname : hostnames) {
            // Remove infiniband suffix
            if (hostname.endsWith(IB_SUFFIX)) {
                hostname = hostname.substring(0, hostname.lastIndexOf(IB_SUFFIX));
            }

            // Add one host name per process to launch
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(hostname);
                for (int i = 1; i < computingUnits; ++i) {
                    hostnamesSTR.append(",").append(hostname);
                }
            } else {
                for (int i = 0; i < computingUnits; ++i) {
                    hostnamesSTR.append(",").append(hostname);
                }
            }
        }
        return hostnamesSTR.toString();

    }

    private void processParameter(InvocationParam np) throws JobExecutionException {
        // We need to use wrapper classes for basic types, reflection will unwrap automatically
        try {
            context.loadParam(np);
            Object obj = np.getValue();
            switch (np.getType()) {
                case BOOLEAN_T:
                    np.setValueClass(boolean.class);
                    break;
                case CHAR_T:
                    np.setValueClass(char.class);
                    break;
                case BYTE_T:
                    np.setValueClass(byte.class);
                    break;
                case SHORT_T:
                    np.setValueClass(short.class);
                    break;
                case INT_T:
                    np.setValueClass(int.class);
                    break;
                case LONG_T:
                    np.setValueClass(long.class);
                    break;
                case FLOAT_T:
                    np.setValueClass(float.class);
                    break;
                case DOUBLE_T:
                    np.setValueClass(double.class);
                    break;
                case STRING_T:
                case STRING_64_T:
                case DIRECTORY_T:
                case FILE_T:
                case BINDING_OBJECT_T:
                case EXTERNAL_STREAM_T:
                case EXTERNAL_PSCO_T:
                    np.setValueClass(String.class);
                    break;
                case OBJECT_T:
                case COLLECTION_T:
                case DICT_COLLECTION_T:
                case STREAM_T:
                case PSCO_T:
                    // Get object
                    if (obj != null) {
                        np.setValueClass(obj.getClass());
                    }
                    break;
                case NULL_T:
                    np.setValue("None");
                    break;
                default:
                    throw new JobExecutionException(ERROR_UNKNOWN_TYPE + np.getType());
            }
        } catch (Exception e) {
            throw new JobExecutionException(e.getMessage(), e);
        }
    }

    /**
     * Perform the task execution (job).
     *
     * @param runner Element hosting the code execution
     * @throws JobExecutionException When an error in the task execution occurs.
     * @throws COMPSsException When the task needs to be stopped (task groups, failure management).
     */
    public void runInvocation(InvocationRunner runner) throws JobExecutionException, COMPSsException {
        this.runner = runner;
        invoke();
    }

    /**
     * Serialize the exit value in the task execution return parameter location.
     *
     * @param returnParam Task execution return parameter
     * @param exitValue Exit value
     * @throws JobExecutionException Exception serializing the exist value.
     */
    public void serializeBinaryExitValue(InvocationParam returnParam, Object exitValue) throws JobExecutionException {
        LOGGER.debug("Checking binary exit value serialization");
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("- Param Type: " + returnParam.getType().name());
            LOGGER.debug("- Preserve source data: " + returnParam.isPreserveSourceData());
            LOGGER.debug("- Write final value: " + returnParam.isWriteFinalValue());
            LOGGER.debug("- Prefix: " + returnParam.getPrefix());
        }

        if (returnParam.getType().equals(DataType.FILE_T)) {
            // Write exit value to the file
            String renaming = returnParam.getOriginalName();
            LOGGER.info("Writing Binary Exit Value (" + exitValue.toString() + ") to " + renaming);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(renaming))) {
                String value = "0000I" + exitValue + "\n.\n";
                writer.write(value);
                writer.flush();
            } catch (IOException ioe) {
                throw new JobExecutionException("ERROR: Cannot serialize binary exit value for bindings", ioe);
            }
        }
    }

    private void invoke() throws JobExecutionException, COMPSsException {
        emitStartTask();
        try {
            AbstractMethodImplementation impl = this.invocation.getMethodImplementation();
            ImplementationDescription desc = impl.getDescription();

            setEnvironmentVariables();
            ExecType prolog = desc.getProlog();
            executeBinary(prolog);

            invokeMethod();

            ExecType epilog = desc.getEpilog();
            executeBinary(epilog);

        } catch (JobExecutionException jee) {
            throw jee;
        } catch (COMPSsException e) {
            throw e;
        } catch (InvokeExecutionException e) {
            throw new JobExecutionException(e);
        } finally {
            emitEndTask();
        }
    }

    public void cancel() {
        cancelMethod();
    }

    protected void setEnvironmentVariables() {
        // Setup properties
        System.setProperty(COMPSsWorker.COMPSS_TASK_ID, String.valueOf(this.invocation.getTaskId()));
        System.setProperty(COMPSS_NUM_NODES, String.valueOf(this.numWorkers));
        System.setProperty(COMPSS_HOSTNAMES, this.workers);
        System.setProperty(COMPSS_NODES, buildWorkersString(hostnames, 1));
        System.setProperty(COMPSS_NUM_THREADS, String.valueOf(this.computingUnits));
        System.setProperty(COMPSS_NUM_PROCS, "1");
        System.setProperty(OMP_NUM_THREADS, String.valueOf(this.computingUnits));
        if (assignedResources != null) {
            String gpus = String.join(", ", Arrays.stream(this.assignedResources.getAssignedGPUs())
                .mapToObj(String::valueOf).toArray(String[]::new));
            System.setProperty(COMPSS_BINDED_GPUS, gpus);
            String cpus = String.join(", ", Arrays.stream(this.assignedResources.getAssignedCPUs())
                .mapToObj(String::valueOf).toArray(String[]::new));
            System.setProperty(COMPSS_BINDED_CPUS, cpus);
        }
        // LOG ENV VARS
        if (LOGGER.isDebugEnabled()) {
            System.out.println("[INVOKER] COMPSS_HOSTNAMES: " + this.workers);
            System.out.println("[INVOKER] COMPSS_NUM_NODES: " + this.numWorkers);
            System.out.println("[INVOKER] COMPSS_NUM_THREADS: " + this.computingUnits);
            System.out.println("[INVOKER] COMPSS_NUM_PROCS: 1");
        }
    }

    private void emitStartTask() {
        if (Tracer.isActivated()) {
            // +1 Because Invocation ID can't be 0 (0 signals end task)
            int coreId = this.invocation.getMethodImplementation().getCoreId() + 1;
            int taskId = this.invocation.getTaskId();
            Tracer.emitEventAndCounters(TraceEventType.TASKS_FUNC, coreId);
            Tracer.emitEvent(TraceEventType.TASKS_ID, taskId);
        }
    }

    private void emitEndTask() {
        if (Tracer.isActivated()) {
            Tracer.emitEventEndAndCounters(TraceEventType.TASKS_FUNC);
            Tracer.emitEventEnd(TraceEventType.TASKS_ID);
        }
    }

    protected abstract void invokeMethod() throws JobExecutionException, COMPSsException;

    protected abstract void cancelMethod();

    private Object executeBinary(ExecType executable) throws InvokeExecutionException {
        if (executable == null || !executable.isAssigned()) {
            return new Object();
        }
        BinaryRunner br = new BinaryRunner();
        String[] params = BinaryRunner.buildAppParams(this.invocation.getParams(), executable.getParams(), null);
        String[] cmd = new String[1 + params.length];
        cmd[0] = executable.getBinary();
        System.arraycopy(params, 0, cmd, 1, params.length);
        return br.executeCMD(cmd, new StdIOStream(), this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, executable.isFailByExitValue());
    }

    protected long becomesNestedApplication(String parallelismSource) {
        long appId = this.context.getRuntimeAPI().registerApplication(parallelismSource, this);
        LOGGER.info("Job " + this.invocation.getJobId() + " becomes app " + appId);
        for (InvocationParam p : this.invocation.getParams()) {
            handleInputValue(appId, p);
        }
        InvocationParam p = this.invocation.getTarget();
        if (p != null) {
            handleInputValue(appId, p);
        }
        return appId;
    }

    private void handleInputValue(Long appId, InvocationParam p) {
        if (p.isCollective()) {
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) p;
            for (InvocationParam sp : cp.getCollectionParameters()) {
                handleInputValue(appId, sp);
            }
        } else {
            switch (p.getType()) {
                case OBJECT_T:
                case PSCO_T: {
                    Object o = p.getValue();
                    this.context.getRuntimeAPI().registerData(appId, p.getType(), o, p.getSourceDataId());
                }
                    break;
                case FILE_T: {
                    String originalName = p.getOriginalName();
                    this.context.getRuntimeAPI().registerData(appId, p.getType(), originalName, p.getSourceDataId());
                }
                    break;
                default:
                    // Do Nothing
            }
        }

    }

    @Override
    public void stalledApplication() {
        // Resources should be released so other tasks run in the node
        this.runner.stalledCodeExecution();
    }

    @Override
    public void readyToContinue(Semaphore sem) {
        // Resources should be re-acquired to continue the execution
        this.runner.readyToContinueExecution(sem);
    }

    protected void completeNestedApplication(long appId) {
        // Wait for all nested tasks to end
        this.context.getRuntimeAPI().barrier(appId);

        // Handle Output Parameters
        for (InvocationParam p : this.invocation.getParams()) {
            if (p.isWriteFinalValue()) {
                handleOutputValue(appId, p);
            }
        }
        for (InvocationParam p : this.invocation.getResults()) {
            handleOutputValue(appId, p);
        }

        // Removing internal application
        this.context.getRuntimeAPI().deregisterApplication(appId);
    }

    private void handleOutputValue(Long appId, InvocationParam p) {
        if (p.isCollective()) {
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) p;
            for (InvocationParam sp : cp.getCollectionParameters()) {
                handleOutputValue(appId, sp);
            }
        } else {
            switch (p.getType()) {
                case OBJECT_T:
                case PSCO_T: {
                    Object o = p.getValue();
                    String dataId = p.getDataMgmtId();
                    ObjectRegistry or = this.context.getLoaderAPI().getObjectRegistry();
                    if (!or.bindToDataIfExisting(appId, o, dataId)) {
                        Object internal = or.collectObjectLastValue(appId, p.getValue());
                        p.setValue(internal);
                    } else {
                        p.resultIsForwarded();
                    }
                }
                    break;
                case FILE_T: {
                    String originalName = (String) p.getValue();
                    String dataId = p.getDataMgmtId();
                    if (this.context.getRuntimeAPI().bindExistingVersionToData(appId, originalName, dataId)) {
                        p.resultIsForwarded();
                    }
                }
                    break;
                default:
                    // Do Nothing
            }
        }

    }

}
