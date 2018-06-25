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

import es.bsc.compss.exceptions.JobExecutionException;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StubItf;


public abstract class Invoker {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    private static final String ERROR_SERIALIZED_OBJ = "ERROR: Cannot obtain object";
    private static final String ERROR_PERSISTENT_OBJ = "ERROR: Cannot getById persistent object";

    protected static final String ERROR_METHOD_DEFINITION = "Incorrect method definition for task of type ";
    protected static final String ERROR_TASK_EXECUTION = "ERROR: Exception executing task (user code)";
    protected static final String ERROR_UNKNOWN_TYPE = "ERROR: Unrecognised type";



    protected static final String OMP_NUM_THREADS = "OMP_NUM_THREADS";

    protected final InvocationContext context;
    protected final Invocation invocation;
    protected final File taskSandboxWorkingDir;
    protected final int[] assignedCoreUnits;
    private final boolean debug;

    public Invoker(
            InvocationContext context,
            Invocation invocation,
            boolean debug,
            File taskSandboxWorkingDir,
            int[] assignedCoreUnits
    ) throws JobExecutionException {

        this.context = context;
        this.invocation = invocation;
        this.taskSandboxWorkingDir = taskSandboxWorkingDir;
        this.assignedCoreUnits = assignedCoreUnits;

        this.debug = debug;

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
        processParameter(invocation.getTarget());

        /* DEBUG information *************************************** */
        if (this.debug) {
            // Print request information
            System.out.println("WORKER - Parameters of execution:");
            System.out.println("  * Method type: " + impl.getMethodType());
            System.out.println("  * Method definition: " + impl.getMethodDefinition());
            System.out.print("  * Parameter types:");
            for (InvocationParam p : invocation.getParams()) {
                System.out.print(" " + p.getValueClass().getName());
            }
            System.out.println("");

            System.out.print("  * Parameter values:");
            for (InvocationParam p : invocation.getParams()) {
                System.out.print(" " + p.getValue());
            }
            System.out.println("");

            System.out.print("  * Parameter streams:");
            for (InvocationParam p : invocation.getParams()) {
                System.out.print(" " + p.getStream());
            }
            if (invocation.getTarget() != null) {
                System.out.print(" " + invocation.getTarget().getStream());
            }
            System.out.println("");

            System.out.print("  * Parameter prefixes:");
            for (InvocationParam p : invocation.getParams()) {
                System.out.print(" " + p.getPrefix());
            }
            if (invocation.getTarget() != null) {
                System.out.print(" " + invocation.getTarget().getPrefix());
            }
            System.out.println("");

            System.out.println("  * Has Target: " + (invocation.getTarget() != null));
            System.out.println("  * Has Return: " + invocation.getReturn() != null);
        }
    }

    private void processParameter(InvocationParam np) throws JobExecutionException {
        // We need to use wrapper classes for basic types, reflection will unwrap automatically

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
                np.setValueClass(String.class);
                break;
            case FILE_T:
                np.setValueClass(String.class);
                break;
            case OBJECT_T:
                // Get object
                Object obj = this.context.getObject(np.getDataMgmtId());
                // Check if object is null
                if (obj == null) {
                    // Try if renaming refers to a PSCOId that is not catched
                    // This happens when 2 tasks have an INOUT PSCO that is persisted within the 1st task
                    try {
                        obj = this.context.getPersistentObject(np.getDataMgmtId());
                    } catch (StorageException se) {
                        throw new JobExecutionException(ERROR_SERIALIZED_OBJ, se);
                    }
                }

                np.setValue(obj);
                if (obj != null) {
                    np.setValueClass(obj.getClass());
                }
                break;
            case PSCO_T:
                // Get Object
                try {
                    obj = this.context.getPersistentObject(np.getDataMgmtId());
                } catch (StorageException e) {
                    throw new JobExecutionException(ERROR_PERSISTENT_OBJ + " with id " + np.getDataMgmtId(), e);
                }

                np.setValue(obj);
                if (obj != null) {
                    np.setValueClass(obj.getClass());
                }
                break;
            case BINDING_OBJECT_T:
            case EXTERNAL_PSCO_T:
                np.setValueClass(String.class);
                break;
            default:
                throw new JobExecutionException(ERROR_UNKNOWN_TYPE + np.getType());
        }
    }

    public void processTask() throws JobExecutionException {
        /* Invoke the requested method ****************************** */
        Object retValue = invoke();
        if (retValue != null) {
            InvocationParam returnParam = invocation.getReturn();
            returnParam.setValue(retValue);
            returnParam.setValueClass(retValue.getClass());
        }
        /* Check SCO persistence for return and target ************** */
        storeFinalValues();
    }

    public void serializeBinaryExitValue() throws JobExecutionException {
        LOGGER.debug("Checking binary exit value serialization");

        InvocationParam returnParam = invocation.getReturn();
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
            LOGGER.info("Writing Binary Exit Value (" + this.invocation.getReturn().getValue().toString() + ") to " + renaming);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(renaming))) {
                String value = "I" + this.invocation.getReturn().getValue().toString() + "\n.\n";
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
        }
        if (this.invocation.getReturn() != null) {
            checkSCOPersistence(this.invocation.getReturn());
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
                this.context.storePersistentObject(id, obj);
                np.setValue(id);
                np.setType(DataType.PSCO_T);
            }
        }
    }

    private void storeValue(InvocationParam np) {
        if (np.isWriteFinalValue()) {
            if (np.getType() != DataType.PSCO_T) {
                //Has already been stored
                this.context.storeObject(np.getDataMgmtId(), np.getValue());
            }
        }
    }

    private Object invoke() throws JobExecutionException {
        emitStartTask();
        try {
            return invokeMethod();
        } catch (JobExecutionException jee) {
            throw jee;
        } finally {
            emitEndTask();
        }
    }

    private void emitStartTask() {
        int taskType = this.invocation.getTaskType() + 1; // +1 Because Invocation ID can't be 0 (0 signals end task)
        int taskId = this.invocation.getTaskId();

        // TRACING: Emit start task
        if (Tracer.isActivated()) {
            Tracer.emitEventAndCounters(taskType, Tracer.getTaskEventsType());
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

    public abstract Object invokeMethod() throws JobExecutionException;

}
