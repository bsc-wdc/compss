/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.external;

import es.bsc.compss.api.Workflow;
import es.bsc.compss.exceptions.WorkflowException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.executor.types.ExternalTaskStatus;
import es.bsc.compss.executor.types.ParameterResult;
import es.bsc.compss.executor.types.ParameterResult.CollectiveResult;
import es.bsc.compss.executor.types.ParameterResult.SingleResult;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.wdc.tracing.Tracer;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Base class of the invokers that delegate the task to a language binding outside the JVM. The invocation is encoded as
 * a command string (built here from the subclass-provided execution command) that the binding consumes through the
 * subclass-specific transport, and the status the binding reports drives the task-result handling shared by all the
 * binding invoker families.
 */
public abstract class BindingInvoker extends Invoker<Workflow> {

    private static final String ERROR_UNSUPPORTED_JOB_TYPE = "Bindings don't support non-native tasks";

    protected static final String SUFFIX_OUT = ".out";
    protected static final String SUFFIX_ERR = ".err";

    protected final ExecuteTaskExternalCommand command;


    /**
     * Binding Invoker constructor. Builds the task command in two steps driven by this constructor: first the
     * subclass-specific execution command obtained through getTaskExecutionCommand, then the generic external arguments
     * appended by appendOtherExecutionCommandArguments.
     *
     * @param context Task execution context
     * @param invocation Task execution description
     * @param sandbox Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating the Binding invoker
     */
    public BindingInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);
        String sandboxPath = sandbox.getFolder().getAbsolutePath();
        this.command = getTaskExecutionCommand(context, invocation, sandboxPath, assignedResources);
        appendOtherExecutionCommandArguments();

    }

    /**
     * Appends to the command the generic external arguments (task description, thread and GPU affinities, hostlist and
     * parameters) and prepends the standard streams file names. Runs as the final step of the command construction from
     * the BindingInvoker constructor, after the subclass has provided the execution command through
     * getTaskExecutionCommand; subclasses never invoke this method themselves.
     *
     * @throws JobExecutionException Error retrieving the external command arguments
     */
    protected void appendOtherExecutionCommandArguments() throws JobExecutionException {

        this.command.appendAllArguments(getExternalCommand(invocation, context, assignedResources));
        String streamsName = context.getStandardStreamsPath(invocation);
        this.command.prependArgument(streamsName + SUFFIX_ERR);
        this.command.prependArgument(streamsName + SUFFIX_OUT);
    }

    protected abstract ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context,
        Invocation invocation, String sandBox, InvocationResources assignedResources);

    private ArrayList<String> getExternalCommand(Invocation invocation, InvocationContext context,
        InvocationResources assignedResources) throws JobExecutionException {

        ArrayList<String> args = new ArrayList<>();

        args.addAll(addArguments(context, invocation));
        args.addAll(addThreadAffinity(assignedResources));
        args.addAll(addGPUAffinity(assignedResources));
        args.addAll(addHostlist(invocation));

        return args;
    }

    private ArrayList<String> addArguments(InvocationContext context, Invocation invocation)
        throws JobExecutionException {
        // The implementation to execute externally can only be METHOD, MULTI_NODE or PYTHON_MPI but we double check it
        if (invocation.getMethodImplementation().getMethodType() != MethodType.METHOD
            && invocation.getMethodImplementation().getMethodType() != MethodType.MULTI_NODE
            && invocation.getMethodImplementation().getMethodType() != MethodType.PYTHON_MPI) {
            throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }

        // Add general task arguments
        ArrayList<String> lArgs = new ArrayList<>();
        lArgs.add(Boolean.toString(Tracer.isExtraeActivated()));
        lArgs.add(Integer.toString(invocation.getTaskId()));
        lArgs.add(Boolean.toString(invocation.isDebugEnabled()));
        lArgs.add(context.getStorageConf());

        // Add method classname and methodName
        MethodType methodType = invocation.getMethodImplementation().getMethodType();
        String methodClass;
        String methodName;
        String ppn = "1";
        switch (methodType) {
            case METHOD:
                MethodDefinition methodImpl = (MethodDefinition) invocation.getMethodImplementation().getDefinition();
                methodClass = methodImpl.getDeclaringClass();
                methodName = methodImpl.getAlternativeMethodName();

                break;
            case PYTHON_MPI:
                PythonMPIDefinition pythonMPIImpl =
                    (PythonMPIDefinition) invocation.getMethodImplementation().getDefinition();
                methodClass = pythonMPIImpl.getDeclaringClass();
                methodName = pythonMPIImpl.getAlternativeMethodName();
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl =
                    (MultiNodeDefinition) invocation.getMethodImplementation().getDefinition();
                methodClass = multiNodeImpl.getDeclaringClass();
                methodName = multiNodeImpl.getMethodName();
                ppn = String.valueOf(multiNodeImpl.getPPN());
                break;
            default:
                throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }
        lArgs.add(String.valueOf(methodType));
        lArgs.add(methodClass);
        lArgs.add(methodName);
        lArgs.add(String.valueOf(invocation.getTimeOut()));

        // Slave nodes and cus description
        List<String> nodeNames = getNodeNames(context, invocation);
        lArgs.add(ppn);
        lArgs.add(String.valueOf(nodeNames.size()));
        lArgs.addAll(nodeNames);
        MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
        lArgs.add(String.valueOf(getNumThreads(context, invocation)));

        // Add hasTarget
        lArgs.add(Boolean.toString(invocation.getTarget() != null));

        // Add return type
        if (!invocation.getResults().isEmpty()) {
            DataType returnType = invocation.getResults().get(0).getType();
            lArgs.add(Integer.toString(returnType.ordinal()));
        } else {
            lArgs.add("null");
        }
        lArgs.add(Integer.toString(invocation.getResults().size()));

        ArrayList<String> invArgs = new ArrayList<>();
        int numParams = invocation.getParams().size();
        // Add parameters
        for (InvocationParam np : invocation.getParams()) {
            invArgs.addAll(convertParameter(np));
        }

        // Add target
        if (invocation.getTarget() != null) {
            numParams++;
            invArgs.addAll(convertParameter(invocation.getTarget()));
        }

        for (InvocationParam np : invocation.getResults()) {
            numParams++;
            invArgs.addAll(convertParameter(np));
        }
        lArgs.add(Integer.toString(numParams));
        lArgs.addAll(invArgs);

        return lArgs;
    }

    protected List<String> getNodeNames(InvocationContext context, Invocation invocation) {
        return invocation.getSlaveNodesNames();
    }

    protected int getNumThreads(InvocationContext context, Invocation invocation) {
        MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
        if (invocation.getMethodImplementation().getMethodType() == MethodType.MULTI_NODE) {
            MultiNodeDefinition multiNodeImpl =
                (MultiNodeDefinition) invocation.getMethodImplementation().getDefinition();
            return requirements.getTotalCPUComputingUnits() / multiNodeImpl.getPPN();
        } else {
            return requirements.getTotalCPUComputingUnits();
        }
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<String> convertParameter(InvocationParam np) {
        ArrayList<String> paramArgs = new ArrayList<>();

        DataType type = np.getType();
        paramArgs.add(Integer.toString(type.ordinal()));
        paramArgs.add(Integer.toString(np.getStdIOStream().ordinal()));
        paramArgs.add(np.getPrefix());
        String name = np.getName();
        String contentType = np.getContentType();
        if (name == null || name.isEmpty()) {
            paramArgs.add("null");
        } else {
            paramArgs.add(name);
        }
        if (contentType == null || contentType.isEmpty()) {
            paramArgs.add("null");
        } else {
            paramArgs.add(contentType);
        }
        switch (type) {
            case FILE_T:
                // Passing originalName link instead of renamed file
                // String originalFile = np.getOriginalName();
                String originalFile = "null";
                if (np.getSourceDataId() != null) {
                    originalFile = np.getOriginalName();
                }
                String destFile = new File(np.getRenamedName()).getName();
                if (!isRuntimeRenamed(destFile)) {
                    // Treat corner case: Destfile is original name. Parameter is INPUT with shared disk, so
                    // destfile should be the same as the input.
                    destFile = originalFile;
                }
                paramArgs.add(originalFile + ":" + destFile + ":" + np.isPreserveSourceData() + ":"
                    + np.isWriteFinalValue() + ":" + np.getOriginalName());
                break;
            case OBJECT_T:
            case PSCO_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case EXTERNAL_PSCO_T:
                paramArgs.add(np.getValue().toString());
                paramArgs.add(np.isWriteFinalValue() ? "W" : "R");
                break;
            case BINDING_OBJECT_T:
                String extObjValue = np.getValue().toString();
                logger.debug("Generating command args for Binding_object {}", extObjValue);
                BindingObject bo = BindingObject.generate(extObjValue);

                String originalData = "";
                if (np.getSourceDataId() != null) { // IN or INOUT
                    originalData = np.getSourceDataId();
                }

                String destData = bo.getName();
                if (!isRuntimeRenamed(destData)) {
                    // TODO: check if it happens also with binding_objects
                    // Corner case: destData is original name. Parameter is IN with shared disk, so
                    // destfile should be the same as the input.
                    destData = originalData;
                }
                paramArgs.add(originalData + ":" + destData + ":" + np.isPreserveSourceData() + ":"
                    + np.isWriteFinalValue() + ":" + np.getOriginalName());
                paramArgs.add(Integer.toString(bo.getType()));
                paramArgs.add(Integer.toString(bo.getElements()));
                break;
            case STRING_64_T:
            case STRING_T:
                String value = np.getValue().toString();
                String[] vals = value.split(" ");
                int numSubStrings = vals.length;
                paramArgs.add(Integer.toString(numSubStrings));
                for (String v : vals) {
                    paramArgs.add(v);
                }
                break;
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                InvocationParamCollection<InvocationParam> ipc = (InvocationParamCollection<InvocationParam>) np;
                writeCollection(ipc);
                paramArgs.add(np.getValue().toString());
                break;
            default:
                paramArgs.add(np.getValue().toString());
        }
        return paramArgs;
    }

    @SuppressWarnings("unchecked")
    private static void writeCollection(InvocationParamCollection<InvocationParam> ipc) {
        String pathToWrite = (String) ipc.getValue();
        logger.debug("Writting Collection file {} ", pathToWrite);
        if (new File(pathToWrite).exists()) {
            logger.debug("Collection file {} already written", pathToWrite);
        } else {
            try (PrintWriter writer = new PrintWriter(pathToWrite, "UTF-8");) {
                for (InvocationParam subParam : ipc.getCollectionParameters()) {
                    int subParamType = subParam.getType().ordinal();
                    Object subParamValue = subParam.getValue();
                    String subParamContentType = subParam.getContentType();
                    writer.println(subParamType + " " + subParamValue + " " + subParamContentType);
                    if (subParam.isCollective()) {
                        writeCollection((InvocationParamCollection<InvocationParam>) subParam);
                    }
                }
            } catch (Exception e) {
                logger.error("Error writting collection to file", e);
                e.printStackTrace(); // NOSONAR need to print in the job out/err
            }
        }
    }

    private static boolean isRuntimeRenamed(String filename) {
        return filename.startsWith("d") && filename.endsWith(".IT");
    }

    /**
     * Builds the affinity description of a set of assigned resource units: the comma-separated identifiers of the
     * units, or the no-affinity placeholder "-" when no unit is assigned.
     *
     * @param assignedUnits Identifiers of the assigned resource units
     * @return The affinity description of the assigned resource units
     */
    private static String buildAffinityString(int[] assignedUnits) {
        if (assignedUnits.length == 0) {
            return "-";
        }

        StringBuilder affinitySTR = new StringBuilder(String.valueOf(assignedUnits[0]));
        for (int i = 1; i < assignedUnits.length; ++i) {
            affinitySTR.append(",").append(assignedUnits[i]);
        }
        return affinitySTR.toString();
    }

    protected ArrayList<String> addThreadAffinity(InvocationResources assignedResources) {
        ArrayList<String> args = new ArrayList<>();
        int[] assignedCoreUnits = assignedResources.getAssignedCPUs();
        args.add(buildAffinityString(assignedCoreUnits));
        return args;
    }

    protected ArrayList<String> addGPUAffinity(InvocationResources assignedResources) {
        ArrayList<String> args = new ArrayList<>();
        int[] assignedGPUs = assignedResources.getAssignedGPUs();
        args.add(buildAffinityString(assignedGPUs));
        return args;
    }

    private ArrayList<String> addHostlist(Invocation invocation) {
        int computingUnits = 0;
        if (invocation.getTaskType() == TaskType.METHOD) {
            MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
            computingUnits = requirements.getTotalCPUComputingUnits();
        }

        ArrayList<String> args = new ArrayList<>();
        String workers = buildWorkersString(hostnames, computingUnits);
        if (workers.isEmpty()) {
            // Placeholder when the hostlist is empty
            args.add("-");
        } else {
            args.add(workers);
        }
        return args;
    }

    @Override
    public final void invokeMethod() throws JobExecutionException, WorkflowException {
        invokeExternalMethod();

        // Remove parameters
        for (InvocationParam np : invocation.getParams()) {
            deleteParameter(np);
        }

        // Remove target
        if (invocation.getTarget() != null) {
            deleteParameter(invocation.getTarget());
        }

        // Remove results
        for (InvocationParam np : invocation.getResults()) {
            deleteParameter(np);
        }
    }

    protected abstract void invokeExternalMethod() throws JobExecutionException, WorkflowException;

    /**
     * Checks the exit value reported by an external task and fails the current job when it is not zero. All the binding
     * invoker families share this contract, so their task-failure reports carry one unified message.
     *
     * @param exitValue Exit value reported by the external task
     * @throws JobExecutionException When the external task reported a non-zero exit value
     */
    protected final void checkExitValue(int exitValue) throws JobExecutionException {
        if (exitValue != 0) {
            throw new JobExecutionException(
                "Job " + this.invocation.getJobId() + " has failed. Exit value is " + exitValue);
        }
    }

    /**
     * Applies the results of an external task to the parameters, the target and the results of the current invocation,
     * in that order. Each application is guarded by the availability of a task result, so bindings reporting fewer
     * entries simply leave the remaining parameters untouched.
     *
     * @param taskStatus Status reported by the external task
     */
    protected void applyTaskResults(ExternalTaskStatus taskStatus) {
        // Update parameters
        logger.debug("Updating parameters for job {}", this.invocation.getJobId());
        Iterator<ParameterResult> taskResults = taskStatus.getResults().iterator();
        for (InvocationParam param : this.invocation.getParams()) {
            if (taskResults.hasNext()) {
                updateParam(param, taskResults.next());
            }
        }

        InvocationParam target = this.invocation.getTarget();
        if (target != null) {
            if (taskResults.hasNext()) {
                updateParam(target, taskResults.next());
            }
        }

        for (InvocationParam param : this.invocation.getResults()) {
            if (taskResults.hasNext()) {
                updateParam(param, taskResults.next());
            }
        }
    }

    /**
     * Updates a parameter with the result reported by an external task. Collective parameters are updated element by
     * element from a collective result; when the result is not collective the parameter is left untouched, since the
     * python bindings report dictionary collections as a single value-less result of the parameter's own type and
     * propagate their updates as versioned files. Simple parameters only adopt the reported value when the result type
     * is a file or an external persistent object.
     *
     * @param param Parameter to update
     * @param result Result reported for the parameter
     */
    protected void updateParam(InvocationParam param, ParameterResult result) {
        if (param.isCollective()) {
            InvocationParamCollection<InvocationParam> cip = (InvocationParamCollection<InvocationParam>) param;
            if (result.isCollective()) {
                CollectiveResult cr = (CollectiveResult) result;
                updateCollectionParam(cip, cr);
            } else {
                logger.debug("Job {} reports no collective result for a collective parameter; leaving it untouched",
                    this.invocation.getJobId());
            }
        } else {
            DataType resultType = result.getType();
            String value = ((SingleResult) result).getValue();
            if (value != null) {
                if (resultType == DataType.EXTERNAL_PSCO_T || resultType == DataType.FILE_T) {
                    param.setType(resultType);
                    param.setValue(value);
                }
            }
        }
    }

    private void updateCollectionParam(InvocationParamCollection<InvocationParam> param, CollectiveResult res) {
        Iterator<InvocationParam> cipItr = param.getCollectionParameters().iterator();
        Iterator<ParameterResult> crItr = res.getElements().iterator();
        while (cipItr.hasNext()) {
            if (crItr.hasNext()) {
                updateParam(cipItr.next(), crItr.next());
            }
        }
    }

    private static void deleteParameter(InvocationParam np) {
        if (np.isCollective()) {
            InvocationParamCollection<InvocationParam> ipc = (InvocationParamCollection<InvocationParam>) np;
            deleteCollectionParameter(ipc);
        }
    }

    @SuppressWarnings("unchecked")
    private static void deleteCollectionParameter(InvocationParamCollection<InvocationParam> ipc) {
        String pathToWrite = (String) ipc.getValue();
        logger.debug("Removing Collection file {} ", pathToWrite);
        (new File(pathToWrite)).delete();

        for (InvocationParam subParam : ipc.getCollectionParameters()) {
            if (subParam.isCollective()) {
                deleteCollectionParameter((InvocationParamCollection<InvocationParam>) subParam);
            }
        }
    }

}
