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
package es.bsc.compss.invokers.external;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.external.ExternalCommand.ExecuteTaskExternalCommand;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 *
 * @author flordan
 */
public abstract class ExternalInvoker extends Invoker {

    private static final String ERROR_UNSUPPORTED_JOB_TYPE = "Bindings don't support non-native tasks";

    protected static final String SUFFIX_OUT = ".out";
    protected static final String SUFFIX_ERR = ".err";

    protected final ExecuteTaskExternalCommand command;

    public ExternalInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources)
            throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        command = getTaskExecutionCommand(context, invocation, taskSandboxWorkingDir.getAbsolutePath(), assignedResources);
        command.appendAllTail(getExternalCommand(invocation, context, assignedResources));
        String streamsName = context.getStandardStreamsPath(invocation);
        command.appendHeadArgument(streamsName + SUFFIX_ERR);
        command.appendHeadArgument(streamsName + SUFFIX_OUT);

    }

    protected abstract ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation, String sandBox, InvocationResources assignedResources);

    private static ArrayList<String> getExternalCommand(Invocation invocation, InvocationContext context, InvocationResources assignedResources)
            throws JobExecutionException {
        ArrayList<String> args = new ArrayList<>();

        args.addAll(addArguments(context, invocation));
        args.addAll(addThreadAffinity(assignedResources));
        args.addAll(addGPUAffinity(assignedResources));
        args.addAll(addHostlist(context, invocation));

        return args;
    }

    private static ArrayList<String> addArguments(InvocationContext context, Invocation invocation) throws JobExecutionException {
        ArrayList<String> lArgs = new ArrayList<>();
        lArgs.add(Boolean.toString(Tracer.isActivated()));
        lArgs.add(Integer.toString(invocation.getTaskId()));
        lArgs.add(Boolean.toString(invocation.isDebugEnabled()));
        lArgs.add(context.getStorageConf());

        // The implementation to execute externally can only be METHOD but we double check it
        if (invocation.getMethodImplementation().getMethodType() != AbstractMethodImplementation.MethodType.METHOD) {
            throw new JobExecutionException(ERROR_UNSUPPORTED_JOB_TYPE);
        }

        // Add method classname and methodname
        MethodImplementation impl = (MethodImplementation) invocation.getMethodImplementation();
        lArgs.add(String.valueOf(impl.getMethodType()));
        lArgs.add(impl.getDeclaringClass());
        lArgs.add(impl.getAlternativeMethodName());

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

        if (invocation.getLang() != es.bsc.compss.COMPSsConstants.Lang.PYTHON) {
            // Add target
            if (invocation.getTarget() != null) {
                numParams++;
                invArgs.addAll(convertParameter(invocation.getTarget()));
            }
        }
        for (InvocationParam np : invocation.getResults()) {
            numParams++;
            invArgs.addAll(convertParameter(np));
        }
        if (invocation.getLang() == es.bsc.compss.COMPSsConstants.Lang.PYTHON) {
            // Add target
            if (invocation.getTarget() != null) {
                numParams++;
                invArgs.addAll(convertParameter(invocation.getTarget()));
            }
        }
        lArgs.add(Integer.toString(numParams));
        lArgs.addAll(invArgs);

        return lArgs;
    }

    private static ArrayList<String> convertParameter(InvocationParam np) {
        ArrayList<String> paramArgs = new ArrayList<>();
        DataType type = np.getType();
        paramArgs.add(Integer.toString(type.ordinal()));
        paramArgs.add(Integer.toString(np.getStream().ordinal()));
        paramArgs.add(np.getPrefix());
        switch (type) {
            case FILE_T:
                // Passing originalName link instead of renamed file

                String originalFile = np.getOriginalName();
                String destFile = new File(np.getValue().toString()).getName();
                if (!isRuntimeRenamed(destFile)) {
                    // Treat corner case: Destfile is original name. Parameter is INPUT with shared disk, so
                    // destfile should be the same as the input.
                    destFile = originalFile;
                }
                paramArgs.add(originalFile + ":" + destFile + ":" + np.isPreserveSourceData() + ":" + np.isWriteFinalValue() + ":"
                        + np.getOriginalName());
                break;
            case OBJECT_T:
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                paramArgs.add(np.getValue().toString());
                paramArgs.add(np.isWriteFinalValue() ? "W" : "R");
                break;
            case BINDING_OBJECT_T:
                String extObjValue = np.getValue().toString();
                LOGGER.debug("Generating command args for Binding_object " + extObjValue);
                BindingObject bo = BindingObject.generate(extObjValue);
                String originalData = np.getOriginalName();
                String destData = bo.getName();
                if (!isRuntimeRenamed(destData)) {
                    // TODO: check if it happens also with binding_objects
                    // Corner case: destData is original name. Parameter is IN with shared disk, so
                    // destfile should be the same as the input.
                    destData = originalData;
                }
                paramArgs.add(originalData + ":" + destData + ":" + np.isPreserveSourceData() + ":" + np.isWriteFinalValue() + ":" + np.getOriginalName());
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
            default:
                paramArgs.add(np.getValue().toString());
        }
        return paramArgs;
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
        if (invocation.getTaskType() == Implementation.TaskType.METHOD) {
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
