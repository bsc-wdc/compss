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
package es.bsc.compss.invokers.binary;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.StdIOStream;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.implementations.ContainerImplementation.ContainerExecutionType;
import es.bsc.compss.types.resources.ContainerDescription;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;


public class ContainerInvoker extends Invoker {

    private static final int NUM_BASE_DOCKER_PYTHON_ARGS = 20;
    private static final int NUM_BASE_DOCKER_BINARY_ARGS = 10;
    private static final int NUM_BASE_SINGULARITY_PYTHON_ARGS = 17;
    private static final int NUM_BASE_SINGULARITY_BINARY_ARGS = 8;

    private static final String REL_PATH_WD = ".." + File.separator + ".." + File.separator;
    private static final String REL_PATH_WORKER_CONTAINER =
        File.separator + "worker" + File.separator + "container" + File.separator + "container_worker.py";

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
        ContainerImplementation containerImpl = null;
        try {
            containerImpl = (ContainerImplementation) invocation.getMethodImplementation();
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
                    pythonInterpreter = pp.getPythonInterpreter();
                }
                this.br.closeStreams(this.invocation.getParams(), pythonInterpreter);
            }
        } catch (StreamCloseException se) {
            LOGGER.error("Exception closing binary streams", se);
            throw new JobExecutionException(se);
        }

        // Update container results
        for (InvocationParam np : this.invocation.getResults()) {
            if (np.getType() == DataType.FILE_T) {
                serializeBinaryExitValue(np, retValue);
            } else {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
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
            pythonInterpreter = pp.getPythonInterpreter();
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
        pyCompssDir = pyCompssDir + "Bindings" + File.separator + "python" + File.separator + pythonVersion
            + File.separator + "pycompss";

        // Setup arguments
        StdIOStream streamValues = new StdIOStream();
        List<String> containerCallParams = new ArrayList<>();
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                // Format parameters
                for (int i = 0; i < this.invocation.getParams().size(); ++i) {
                    InvocationParam userParam = this.invocation.getParams().get(i);
                    containerCallParams.add(String.valueOf(userParam.getType().ordinal()));
                    containerCallParams.add(String.valueOf(userParam.getStdIOStream().ordinal()));
                    containerCallParams.add(userParam.getPrefix());
                    containerCallParams.add(userParam.getName());
                    containerCallParams.add("null");
                    String value = String.valueOf(userParam.getValue());
                    if (userParam.getType().equals(DataType.STRING_T)) {
                        // TODO: Support more sub-strings
                        containerCallParams.add("1");
                        String encodedValue = Base64.getEncoder().encodeToString(value.getBytes());
                        containerCallParams.add(encodedValue);
                    } else {
                        containerCallParams.add(value);
                    }
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
                        cmd[cmdIndex++] = pyCompssDir + ":" + pyCompssDir;
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
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        cmd[cmdIndex++] = "SINGULARITYENV_PYTHONPATH=" + pythonPath + ":" + pyCompssDir;
                        break;
                    case CET_BINARY:
                        // Nothing to add
                        break;
                }
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
                cmd[cmdIndex++] = "python";
                cmd[cmdIndex++] = pyCompssDir + REL_PATH_WORKER_CONTAINER;
                cmd[cmdIndex++] = userModule;
                cmd[cmdIndex++] = userFunction;
                cmd[cmdIndex++] = String.valueOf(this.invocation.getParams().size());
                break;
            case CET_BINARY:
                cmd[cmdIndex++] = this.internalBinary;
                break;
        }

        // Prepare command - Prepare user arguments
        for (int i = 0; i < containerCallParams.size(); ++i) {
            cmd[cmdIndex++] = containerCallParams.get(i);
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
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.failByEV);
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling binary process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
