/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.binary;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.InvocationParamDictCollection;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition.ContainerExecutionType;
import es.bsc.compss.types.resources.ContainerDescription;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;


public class ContainerInvoker extends Invoker {

    private static final int NUM_BASE_DOCKER_PYTHON_ARGS = 25;
    private static final int NUM_BASE_DOCKER_BINARY_ARGS = 10;
    private static final int NUM_BASE_SINGULARITY_PYTHON_ARGS = 21;
    private static final int NUM_BASE_SINGULARITY_BINARY_ARGS = 8;

    private static final String REL_PATH_WD = ".." + File.separator + ".." + File.separator;
    private static final String REL_PATH_WORKER_CONTAINER = File.separator + "pycompss" + File.separator + "worker"
        + File.separator + "container" + File.separator + "container_worker.py";

    private final ContainerDescription container;
    private final ContainerExecutionType internalExecutionType;
    private final String internalBinary;
    private final String internalFunction;

    private final String customWorkingDir;
    private final boolean failByEV;

    private BinaryRunner br;


    /**
     * Binary Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param taskSandboxWorkingDir Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the binary invoker.
     */
    public ContainerInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        ContainerDefinition containerImpl = null;
        try {
            containerImpl = (ContainerDefinition) invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }

        this.container = containerImpl.getContainer();
        this.internalExecutionType = containerImpl.getInternalExecutionType();
        this.internalBinary = containerImpl.getInternalBinary();
        this.internalFunction = containerImpl.getInternalFunction();

        this.customWorkingDir = containerImpl.getWorkingDir();
        this.failByEV = containerImpl.isFailByEV();

        // Internal binary runner
        this.br = null;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked Container execution (internalType = " + this.internalExecutionType + ", internalBinary = "
            + this.internalBinary + ", internalFunction = " + this.internalFunction + ") in "
            + this.context.getHostName());

        // Execute container
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            LOGGER.error("Exception running container", iee);
            throw new JobExecutionException(iee);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                String pythonInterpreter = null;
                LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
                if (lp instanceof PythonParams) {
                    PythonParams pp = (PythonParams) lp;
                    pythonInterpreter = pp.checkCoverageAndGetPythonInterpreter();
                }
                this.br.closeStreams(this.invocation.getParams(), pythonInterpreter);
            }
        } catch (StreamCloseException se) {
            LOGGER.error("Exception closing binary streams", se);
            throw new JobExecutionException(se);
        }

        // Update container results
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                // Real return value is updated in the container_worker.py
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Python Container Execution of Job " + this.invocation.getJobId() + " (Task "
                        + this.invocation.getTaskId() + ") has exit value " + retValue.toString());
                }
                break;
            case CET_BINARY:
                // Update the result parameters with the binary exit value
                for (InvocationParam np : this.invocation.getResults()) {
                    if (np.getType() == DataType.FILE_T) {
                        serializeBinaryExitValue(np, retValue);
                    } else {
                        np.setValue(retValue);
                        np.setValueClass(retValue.getClass());
                    }
                }
                break;
        }

        // Mark job as failed if needed
        if (this.failByEV) {
            if (!retValue.toString().equals("0")) {
                throw new JobExecutionException("Received non-zero exit value (" + retValue.toString() + ") for Job "
                    + this.invocation.getJobId() + " (Task " + this.invocation.getTaskId() + ")");
            }
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        // Get python interpreter and required directories
        String pythonInterpreter = null;
        String pythonVersion = null;
        String pythonPath = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.checkCoverageAndGetPythonInterpreter();
            pythonVersion = pp.getPythonVersion();
            pythonPath = pp.getPythonPath();
        }

        // Setup working directory and mountpoint
        String workingDir;
        if (this.customWorkingDir != null && !this.customWorkingDir.isEmpty()
            && !this.customWorkingDir.equals("[unassigned]")) {
            // Container working directory specified by the user
            workingDir = this.customWorkingDir;
        } else {
            // Container working directory to worker sandbox working directory
            workingDir = this.taskSandboxWorkingDir.getAbsolutePath();
        }
        workingDir = workingDir.endsWith(File.separator) ? workingDir : workingDir + File.separator;

        String workingDirMountPoint = workingDir;
        if (workingDirMountPoint.contains("sandBox")) {
            workingDirMountPoint = workingDirMountPoint + REL_PATH_WD;
        }

        // Setup application directory
        String appDir = this.context.getAppDir();
        appDir = appDir.endsWith(File.separator) ? appDir : appDir + File.separator;

        // Setup PyCOMPSs directory
        String pyCompssDir = this.context.getInstallDir();
        pyCompssDir = pyCompssDir.endsWith(File.separator) ? pyCompssDir : pyCompssDir + File.separator;
        pyCompssDir = pyCompssDir + "Bindings" + File.separator + "python" + File.separator + "3";

        // Setup Python CET execution flags
        boolean hasTarget = false;
        String returnType = "null";
        int returnLength = 0;
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                hasTarget = (this.invocation.getTarget() != null);
                List<? extends InvocationParam> results = this.invocation.getResults();
                if (results != null && !results.isEmpty()) {
                    returnType = String.valueOf(DataType.FILE_T.ordinal());
                    returnLength = results.size();
                } else {
                    // Default values
                }
                break;

            case CET_BINARY:
                // Default values
                break;
        }

        // Setup arguments
        StdIOStream streamValues = new StdIOStream();
        List<String> containerCallParams = new ArrayList<>();
        int numContainerCallParams = 0;
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                // Format parameters
                if (hasTarget) {
                    addParamInfo(containerCallParams, this.invocation.getTarget());
                    numContainerCallParams++;
                }
                if (returnLength > 0) {
                    for (int i = 0; i < this.invocation.getResults().size(); ++i) {
                        InvocationParam userParam = this.invocation.getResults().get(i);
                        addParamInfo(containerCallParams, userParam);
                        numContainerCallParams++;
                    }
                }
                for (int i = 0; i < this.invocation.getParams().size(); ++i) {
                    InvocationParam userParam = this.invocation.getParams().get(i);
                    addParamInfo(containerCallParams, userParam);
                    numContainerCallParams++;
                }
                break;
            case CET_BINARY:
                // Convert binary parameters and calculate binary-streams redirection - binary execution
                containerCallParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
                    this.invocation.getTarget(), streamValues, pythonInterpreter);
                break;
        }

        // Prepare command - Determine length of the command
        int numCmdArgs = 0;
        switch (this.container.getEngine()) {
            case DOCKER:
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        numCmdArgs = NUM_BASE_DOCKER_PYTHON_ARGS + containerCallParams.size();
                        break;
                    case CET_BINARY:
                        numCmdArgs = NUM_BASE_DOCKER_BINARY_ARGS + containerCallParams.size();
                }
                break;
            case SINGULARITY:
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        numCmdArgs = NUM_BASE_SINGULARITY_PYTHON_ARGS + containerCallParams.size();
                        break;
                    case CET_BINARY:
                        numCmdArgs = NUM_BASE_SINGULARITY_BINARY_ARGS + containerCallParams.size();
                }
        }

        String[] cmd = new String[numCmdArgs];
        int cmdIndex = 0;

        // Prepare command - Determine base of the command and container binds
        switch (this.container.getEngine()) {
            case DOCKER:
                cmd[cmdIndex++] = "docker";
                cmd[cmdIndex++] = "run";
                cmd[cmdIndex++] = "-i";
                cmd[cmdIndex++] = "--rm";
                cmd[cmdIndex++] = "-v";
                cmd[cmdIndex++] = workingDirMountPoint + ":" + workingDirMountPoint;
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        cmd[cmdIndex++] = "-v";
                        cmd[cmdIndex++] = appDir + ":" + appDir;
                        cmd[cmdIndex++] = "-v";
                        cmd[cmdIndex++] = pyCompssDir + File.separator + "pycompss" + File.separator + ":" + pyCompssDir
                            + File.separator + "pycompss" + File.separator;
                        cmd[cmdIndex++] = "--env";
                        cmd[cmdIndex++] = "PYTHONPATH=" + pythonPath + ":" + pyCompssDir;
                        break;
                    case CET_BINARY:
                        // Nothing to add
                        break;
                }
                cmd[cmdIndex++] = "-w";
                cmd[cmdIndex++] = workingDir;
                cmd[cmdIndex++] = this.container.getImage();
                break;

            case SINGULARITY:
                cmd[cmdIndex++] = "singularity";
                cmd[cmdIndex++] = "exec";
                cmd[cmdIndex++] = "--bind";
                cmd[cmdIndex++] = workingDirMountPoint + ":" + workingDirMountPoint;
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        cmd[cmdIndex++] = "--bind";
                        cmd[cmdIndex++] = appDir + ":" + appDir;
                        cmd[cmdIndex++] = "--bind";
                        cmd[cmdIndex++] = pyCompssDir + ":" + pyCompssDir;
                        break;
                    case CET_BINARY:
                        // Nothing to add
                        break;
                }
                cmd[cmdIndex++] = "--pwd";
                cmd[cmdIndex++] = workingDir;
                cmd[cmdIndex++] = this.container.getImage();
        }

        // Prepare command - Determine execution command (binary or Python module and function)
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                final String[] parts = this.internalFunction.split("&");
                final String userModule = parts[0];
                final String userFunction = parts[1];
                cmd[cmdIndex++] = "python" + pythonVersion;
                cmd[cmdIndex++] = pyCompssDir + REL_PATH_WORKER_CONTAINER;
                cmd[cmdIndex++] = userModule; // 1
                cmd[cmdIndex++] = userFunction; // 2
                if (this.invocation.isDebugEnabled()) {
                    cmd[cmdIndex++] = "true"; // debug 3
                } else {
                    cmd[cmdIndex++] = "false"; // debug 3
                }
                cmd[cmdIndex++] = "false"; // tracing 4
                cmd[cmdIndex++] = String.valueOf(hasTarget); // 5
                cmd[cmdIndex++] = returnType; // 6
                cmd[cmdIndex++] = String.valueOf(returnLength); // 7
                cmd[cmdIndex++] = String.valueOf(numContainerCallParams); // 8
                break;
            case CET_BINARY:
                cmd[cmdIndex++] = this.internalBinary;
                break;
        }

        // Prepare command - Prepare user arguments
        for (int i = 0; i < containerCallParams.size(); ++i) {
            cmd[cmdIndex++] = containerCallParams.get(i); // 9:
        }

        // Debug information
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println("[CONTAINER INVOKER] Begin binary call to container execution");
            outLog.println("[CONTAINER INVOKER] Engine: " + this.container.getEngine().toString());
            outLog.println("[CONTAINER INVOKER] Image: " + this.container.getImage());
            outLog.println("[CONTAINER INVOKER] Internal Type: " + this.internalExecutionType);
            outLog.println("[CONTAINER INVOKER] Internal Binary: " + this.internalBinary);
            outLog.println("[CONTAINER INVOKER] Internal Function: " + this.internalFunction);
            outLog.println("[CONTAINER INVOKER] On WorkingDir : " + workingDir);
            // Debug command
            outLog.print("[CONTAINER INVOKER] BINARY CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[CONTAINER INVOKER] Binary STDIN: " + streamValues.getStdIn());
            outLog.println("[CONTAINER INVOKER] Binary STDOUT: " + streamValues.getStdOut());
            outLog.println("[CONTAINER INVOKER] Binary STDERR: " + streamValues.getStdErr());
        }

        // Launch command
        final String completePythonpath = pythonPath + ":" + pyCompssDir;
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), completePythonpath, this.failByEV);
    }

    private void addParamInfo(List<String> paramsList, InvocationParam p) {
        paramsList.addAll(convertParameter(p));
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
        if (name == null || name.isEmpty()) {
            paramArgs.add("null");
        } else {
            paramArgs.add(np.getContentType());
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
            case STRING_64_T:
                // decode the string
                byte[] decodedBytes = Base64.getDecoder().decode(np.getValue().toString());
                String[] values = new String(decodedBytes).split(" ");
                // add total # of strings
                paramArgs.add(Integer.toString(values.length));
                paramArgs.addAll(Arrays.asList(values));
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

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling binary process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
