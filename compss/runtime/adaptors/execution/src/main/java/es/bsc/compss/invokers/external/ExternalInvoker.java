/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.InvocationParamDictCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public abstract class ExternalInvoker extends Invoker {

    private static final String ERROR_UNSUPPORTED_JOB_TYPE = "Bindings don't support non-native tasks";

    protected static final String SUFFIX_OUT = ".out";
    protected static final String SUFFIX_ERR = ".err";

    protected final ExecuteTaskExternalCommand command;


    /**
     * External Invoker constructor.
     *
     * @param context Task execution context
     * @param invocation Task execution description
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating the External invoker
     */
    public ExternalInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        this.command =
            getTaskExecutionCommand(context, invocation, taskSandboxWorkingDir.getAbsolutePath(), assignedResources);
        this.command.appendAllArguments(getExternalCommand(invocation, context, assignedResources));
        String streamsName = context.getStandardStreamsPath(invocation);
        this.command.prependArgument(streamsName + SUFFIX_ERR);
        this.command.prependArgument(streamsName + SUFFIX_OUT);

    }

    protected abstract ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context,
        Invocation invocation, String sandBox, InvocationResources assignedResources);

    private static ArrayList<String> getExternalCommand(Invocation invocation, InvocationContext context,
        InvocationResources assignedResources) throws JobExecutionException {

        ArrayList<String> args = new ArrayList<>();

        args.addAll(addArguments(context, invocation));
        args.addAll(addThreadAffinity(assignedResources));
        args.addAll(addGPUAffinity(assignedResources));
        args.addAll(addHostlist(context, invocation));

        return args;
    }

    private static ArrayList<String> addArguments(InvocationContext context, Invocation invocation)
        throws JobExecutionException {
        // The implementation to execute externally can only be METHOD, MULTI_NODE or PYTHON_MPI but we double check it
        if (invocation.getMethodImplementation().getMethodType() != MethodType.METHOD
            && invocation.getMethodImplementation().getMethodType() != MethodType.MULTI_NODE
            && invocation.getMethodImplementation().getMethodType() != MethodType.PYTHON_MPI) {
            throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }

        // Add general task arguments
        ArrayList<String> lArgs = new ArrayList<>();
        lArgs.add(Integer.toString(Tracer.getLevel()));
        lArgs.add(Integer.toString(invocation.getTaskId()));
        lArgs.add(Boolean.toString(invocation.isDebugEnabled()));
        lArgs.add(context.getStorageConf());

        // Add method classname and methodname
        MethodType methodType = invocation.getMethodImplementation().getMethodType();
        String methodClass;
        String methodName;
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
                break;
            default:
                throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }
        lArgs.add(String.valueOf(methodType));
        lArgs.add(methodClass);
        lArgs.add(methodName);
        lArgs.add(String.valueOf(invocation.getTimeOut()));

        // Slave nodes and cus description
        lArgs.add(String.valueOf(invocation.getSlaveNodesNames().size()));
        lArgs.addAll(invocation.getSlaveNodesNames());
        MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
        lArgs.add(String.valueOf(requirements.getTotalCPUComputingUnits()));

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

    @SuppressWarnings("unchecked")
    private static ArrayList<String> convertParameter(InvocationParam np) {
        ArrayList<String> paramArgs = new ArrayList<>();

        DataType type = np.getType();
        paramArgs.add(Integer.toString(type.ordinal()));
        paramArgs.add(Integer.toString(np.getStdIOStream().ordinal()));
        paramArgs.add(np.getPrefix());
        String name = np.getName();
        if (name == null || name.isEmpty()) {
            paramArgs.add("null");
        } else {
            paramArgs.add(name);
        }
        paramArgs.add(np.getContentType());
        switch (type) {
            case FILE_T:
                // Passing originalName link instead of renamed file
                // String originalFile = np.getOriginalName();
                String originalFile = "null";
                if(np.getSourceDataId() != null) {
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
                LOGGER.debug("Generating command args for Binding_object " + extObjValue);
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
                InvocationParamCollection<InvocationParam> ipc = (InvocationParamCollection<InvocationParam>) np;
                writeCollection(ipc);
                paramArgs.add(np.getValue().toString());
                break;
            case DICT_COLLECTION_T:
                InvocationParamDictCollection<InvocationParam, InvocationParam> ipdc =
                    (InvocationParamDictCollection<InvocationParam, InvocationParam>) np;
                writeDictCollection(ipdc);
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
        LOGGER.debug("Writting Collection file " + pathToWrite + " ");
        if (new File(pathToWrite).exists()) {
            LOGGER.debug("Collection file " + pathToWrite + " already written");
        } else {
            try (PrintWriter writer = new PrintWriter(pathToWrite, "UTF-8");) {
                for (InvocationParam subParam : ipc.getCollectionParameters()) {
                    writer.println(
                        subParam.getType().ordinal() + " " + subParam.getValue() + " " + subParam.getContentType());
                    if (subParam.getType() == DataType.COLLECTION_T) {
                        writeCollection((InvocationParamCollection<InvocationParam>) subParam);
                    } else if (subParam.getType() == DataType.DICT_COLLECTION_T) {
                        writeDictCollection((InvocationParamDictCollection<InvocationParam, InvocationParam>) subParam);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error writting collection to file", e);
                e.printStackTrace(); // NOSONAR need to print in the job out/err
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void writeDictCollection(InvocationParamDictCollection<InvocationParam, InvocationParam> ipdc) {
        String pathToWrite = (String) ipdc.getValue();
        LOGGER.debug("Writting Dictionary Collection file " + pathToWrite + " ");
        if (new File(pathToWrite).exists()) {
            LOGGER.debug("Dictionary Collection file " + pathToWrite + " already written");
        } else {
            try (PrintWriter writer = new PrintWriter(pathToWrite, "UTF-8");) {
                for (Map.Entry<InvocationParam, InvocationParam> entry : ipdc.getDictCollectionParameters()
                    .entrySet()) {
                    InvocationParam subParam = entry.getKey();
                    writer.println(
                        subParam.getType().ordinal() + " " + subParam.getValue() + " " + subParam.getContentType());
                    if (subParam.getType() == DataType.DICT_COLLECTION_T) {
                        writeDictCollection((InvocationParamDictCollection<InvocationParam, InvocationParam>) subParam);
                    } else if (subParam.getType() == DataType.COLLECTION_T) {
                        writeCollection((InvocationParamCollection<InvocationParam>) subParam);
                    }
                    subParam = entry.getValue();
                    writer.println(
                        subParam.getType().ordinal() + " " + subParam.getValue() + " " + subParam.getContentType());
                    if (subParam.getType() == DataType.DICT_COLLECTION_T) {
                        writeDictCollection((InvocationParamDictCollection<InvocationParam, InvocationParam>) subParam);
                    } else if (subParam.getType() == DataType.COLLECTION_T) {
                        writeCollection((InvocationParamCollection<InvocationParam>) subParam);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error writting dictionary collection to file");
                e.printStackTrace();
            }
        }
    }

    private static boolean isRuntimeRenamed(String filename) {
        return filename.startsWith("d") && filename.endsWith(".IT");
    }

    private static ArrayList<String> addThreadAffinity(InvocationResources assignedResources) {
        ArrayList<String> args = new ArrayList<>();
        int[] assignedCoreUnits = assignedResources.getAssignedCPUs();
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
        return args;
    }

    private static ArrayList<String> addGPUAffinity(InvocationResources assignedResources) {
        ArrayList<String> args = new ArrayList<>();
        int[] assignedGPUs = assignedResources.getAssignedGPUs();
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
        return args;
    }

    private static ArrayList<String> addHostlist(InvocationContext context, Invocation invocation) {
        ArrayList<String> args = new ArrayList<>();
        List<String> hostnames = invocation.getSlaveNodesNames();
        hostnames.add(context.getHostName());

        ResourceDescription rd = invocation.getRequirements();
        int computingUnits;
        if (invocation.getTaskType() == TaskType.METHOD) {
            computingUnits = ((MethodResourceDescription) rd).getTotalCPUComputingUnits();
        } else {
            computingUnits = 0;
        }

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
        String workers = hostnamesSTR.toString();

        if (workers != null && !workers.isEmpty()) {
            args.add(workers);
        } else {
            args.add("-");
        }
        return args;
    }
}
