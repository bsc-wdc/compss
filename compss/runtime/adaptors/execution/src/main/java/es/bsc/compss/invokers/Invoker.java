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
package es.bsc.compss.invokers;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.util.Tracer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Invoker {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    protected static final String ERROR_METHOD_DEFINITION = "Incorrect method definition for task of type ";
    protected static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
    protected static final String ERROR_UNKNOWN_TYPE = "ERROR: Unrecognised type";

    protected static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";
    protected static final String COMPSS_HOSTNAMES = "COMPSS_HOSTNAMES";
    protected static final String COMPSS_NUM_NODES = "COMPSS_NUM_NODES";
    protected static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";

    protected final InvocationContext context;
    protected final Invocation invocation;
    protected final File taskSandboxWorkingDir;
    protected final InvocationResources assignedResources;

    protected final int computingUnits;
    protected final String workers;
    protected final int numWorkers;


    public Invoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources)
            throws JobExecutionException {

        this.context = context;
        this.invocation = invocation;
        this.taskSandboxWorkingDir = taskSandboxWorkingDir;
        this.assignedResources = assignedResources;

        /* Parse execution infrastructure **************************************** */
        // ComputingUnits flags
        ResourceDescription rd = this.invocation.getRequirements();
        if (this.invocation.getTaskType() == Implementation.TaskType.METHOD) {
            this.computingUnits = ((MethodResourceDescription) rd).getTotalCPUComputingUnits();
        } else {
            this.computingUnits = 0;
        }

        // Multi-Node flags
        List<String> hostnames = invocation.getSlaveNodesNames();
        hostnames.add(context.getHostName());
        this.numWorkers = hostnames.size();

        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (Iterator<String> it = hostnames.iterator(); it.hasNext();) {
            String hostname = it.next();
            // Remove infiniband suffix
            if (hostname.endsWith("-ib0")) {
                hostname = hostname.substring(0, hostname.lastIndexOf("-ib0"));
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
        this.workers = hostnamesSTR.toString();

        /* Parse the parameters ************************************ */
        AbstractMethodImplementation impl = invocation.getMethodImplementation();
        int paramIdx = 0;
        for (InvocationParam np : invocation.getParams()) {
            processParameter(np);
            // Check if object is still null
            if (np.getValue() == null) {
                StringBuilder sb = new StringBuilder();
                sb.append("Object parameter ").append(paramIdx);
                sb.append(" with renaming ").append(np.getDataMgmtId());
                sb.append(" in MethodDefinition ").append(impl.getMethodDefinition());
                sb.append(" is null!").append("\n");

                throw new JobExecutionException(sb.toString());
            }
            paramIdx++;
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
                out.print(" " + p.getValueClass().getName());
            }
            out.println("");

            out.print("  * Parameter values:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getValue());
            }
            out.println("");

            out.print("  * Parameter streams:");
            for (InvocationParam p : invocation.getParams()) {
                out.print(" " + p.getStream());
            }
            if (invocation.getTarget() != null) {
                out.print(" " + invocation.getTarget().getStream());
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
            out.println("  * Has Return: " + invocation.getResults() != null);
        }
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
                case FILE_T:
                case BINDING_OBJECT_T:
                case EXTERNAL_PSCO_T:
                    np.setValueClass(String.class);
                    break;
                case OBJECT_T:
                case PSCO_T:
                    // Get object
                    if (obj != null) {
                        np.setValueClass(obj.getClass());
                    }
                    break;
                default:
                    throw new JobExecutionException(ERROR_UNKNOWN_TYPE + np.getType());
            }
        } catch (Exception e) {
            throw new JobExecutionException(e.getMessage(), e);
        }
    }

    public void processTask() throws JobExecutionException {
        /* Invoke the requested method ****************************** */
        invoke();
        try {
            storeFinalValues();
        } catch (Exception e) {
            throw new JobExecutionException("Error storing a task result", e);
        }
    }

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

    private void storeFinalValues() throws Exception {
        // Check all parameters and target
        for (InvocationParam np : this.invocation.getParams()) {
            storeValue(np);
        }
        if (this.invocation.getTarget() != null) {
            storeValue(this.invocation.getTarget());
        }
        for (InvocationParam np : this.invocation.getResults()) {
            storeValue(np);
        }
    }

    private void storeValue(InvocationParam np) throws Exception {
        if (np.isWriteFinalValue()) {
            // Has already been stored
            this.context.storeParam(np);
        }
    }

    private void invoke() throws JobExecutionException {
        emitStartTask();
        try {
            setEnvironmentVariables();
            invokeMethod();
        } catch (JobExecutionException jee) {
            throw jee;
        } finally {
            emitEndTask();
        }
    }

    private void setEnvironmentVariables() {
        // Setup properties
        System.setProperty(COMPSS_HOSTNAMES, this.workers);
        System.setProperty(COMPSS_NUM_NODES, String.valueOf(this.numWorkers));
        System.setProperty(COMPSS_NUM_THREADS, String.valueOf(this.computingUnits));
        System.setProperty(OMP_NUM_THREADS, String.valueOf(this.computingUnits));
        
        // LOG ENV VARS
        System.out.println("[INVOKER] COMPSS_HOSTNAMES: " + this.workers);
        System.out.println("[INVOKER] COMPSS_NUM_NODES: " + this.numWorkers);
        System.out.println("[INVOKER] COMPSS_NUM_THREADS: " + this.computingUnits);
    }

    private void emitStartTask() {
        // TRACING: Emit start task
        if (Tracer.isActivated()) {
            int coreId = this.invocation.getMethodImplementation().getCoreId() + 1; // +1 Because Invocation ID can't be
                                                                                    // 0 (0 signals end task)
            int taskId = this.invocation.getTaskId();
            Tracer.emitEventAndCounters(coreId, Tracer.getTaskEventsType());
            Tracer.emitEvent(taskId, Tracer.getTaskSchedulingType());
        }
    }

    private void emitEndTask() {
        // TRACING: Emit end task
        if (Tracer.isActivated()) {
            Tracer.emitEventAndCounters(Tracer.EVENT_END, Tracer.getTaskEventsType());
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTaskSchedulingType());
        }
    }

    protected abstract void invokeMethod() throws JobExecutionException;

    /**
     * Writes the given list of workers to a hostfile inside the given task sandbox
     *
     * @param taskSandboxWorkingDir
     * @param workers
     * @return
     * @throws InvokeExecutionException
     */
    protected static String writeHostfile(File taskSandboxWorkingDir, String workers) throws InvokeExecutionException {
        // Locate hostfile file
        String uuid = UUID.randomUUID().toString();
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + uuid + ".hostfile";

        // Modify the workers' list
        String workersInLines = workers.replace(',', '\n');

        // Write hostfile
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write(workersInLines);
        } catch (IOException ioe) {
            throw new InvokeExecutionException("ERROR: Cannot write hostfile", ioe);
        }
        return filename;
    }

}
