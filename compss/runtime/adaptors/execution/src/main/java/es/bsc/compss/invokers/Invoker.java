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

import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.annotations.Constants;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


public abstract class Invoker {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    protected static final String ERROR_METHOD_DEFINITION = "Incorrect method definition for task of type ";
    protected static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
    protected static final String ERROR_UNKNOWN_TYPE = "ERROR: Unrecognised type";



    protected static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";

    protected final InvocationContext context;
    protected final Invocation invocation;
    protected final File taskSandboxWorkingDir;
    protected final InvocationResources assignedResources;

    public Invoker(
            InvocationContext context,
            Invocation invocation,
            File taskSandboxWorkingDir,
            InvocationResources assignedResources
    ) throws JobExecutionException {

        this.context = context;
        this.invocation = invocation;
        this.taskSandboxWorkingDir = taskSandboxWorkingDir;
        this.assignedResources = assignedResources;

        /* Invocation information **************************************** */
        AbstractMethodImplementation impl = invocation.getMethodImplementation();


        /* Parse the parameters ************************************ */
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

        /* Check SCO persistence for return and target ************** */
        storeFinalValues();
    }

    public void serializeBinaryExitValue() throws JobExecutionException {
        LOGGER.debug("Checking binary exit value serialization");

        InvocationParam returnParam = invocation.getResults().get(0);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("- Param Type: " + returnParam.getType().name());
            LOGGER.debug("- Preserve source data: " + returnParam.isPreserveSourceData());
            LOGGER.debug("- Write final value: " + returnParam.isWriteFinalValue());
            LOGGER.debug("- Prefix: " + returnParam.getPrefix());
        }

        // Last parameter is a FILE, direction OUT, with skip prefix => return in Python
        if (returnParam.getType().equals(DataType.FILE_T) && !returnParam.isPreserveSourceData() && returnParam.isWriteFinalValue()
                && returnParam.getPrefix().equals(Constants.PREFIX_SKIP)) {

            // Write exit value to the file
            String renaming = returnParam.getOriginalName();
            LOGGER.info("Writing Binary Exit Value (" + returnParam.getValue().toString() + ") to " + renaming);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(renaming))) {
                String value = "0000I" + returnParam.getValue().toString() + "\n.\n";
                writer.write(value);
                writer.flush();
            } catch (IOException ioe) {
                throw new JobExecutionException("ERROR: Cannot serialize binary exit value for bindings", ioe);
            }
        }
    }

    private void storeFinalValues() {
        // Check all parameters and target
        for (InvocationParam np : this.invocation.getParams()) {
            checkSCOPersistence(np);
            storeValue(np);
        }
        if (this.invocation.getTarget() != null) {
            checkSCOPersistence(this.invocation.getTarget());
            storeValue(this.invocation.getTarget());
        }
        for (InvocationParam np : this.invocation.getResults()) {
            checkSCOPersistence(np);
            storeValue(np);
        }
    }

    private void checkSCOPersistence(InvocationParam np) {
        boolean potentialPSCO = (np.getType().equals(DataType.OBJECT_T)) || (np.getType().equals(DataType.PSCO_T));
        if (np.isWriteFinalValue() && potentialPSCO) {
            Object obj = np.getValue();

            // Check if it is a PSCO and has been persisted in task
            String id = null;
            try {
                StubItf psco = (StubItf) obj;
                id = psco.getID();
            } catch (Exception e) {
                // No need to raise an exception because normal objects are not PSCOs
                id = null;
            }

            // Update to PSCO if needed
            if (id != null) {
                // Object has been persisted, we store the PSCO and change the value to its ID
                np.setType(DataType.PSCO_T);
            }
        }
    }

    private void storeValue(InvocationParam np) {
        if (np.isWriteFinalValue()) {
            //Has already been stored
            this.context.storeParam(np);
        }
    }

    private void invoke() throws JobExecutionException {
        emitStartTask();
        try {
            invokeMethod();
        } catch (JobExecutionException jee) {
            throw jee;
        } finally {
            emitEndTask();
        }
    }

    private void emitStartTask() {
        int coreId = this.invocation.getMethodImplementation().getCoreId() + 1; // +1 Because Invocation ID can't be 0 (0 signals end task)
        int taskId = this.invocation.getTaskId();

        // TRACING: Emit start task
        if (Tracer.isActivated()) {
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

}
