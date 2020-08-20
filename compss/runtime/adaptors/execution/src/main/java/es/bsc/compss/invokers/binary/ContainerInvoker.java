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


public class ContainerInvoker extends Invoker {

    private static final int NUM_BASE_DOCKER_PYTHON_ARGS = 23;
    private static final int NUM_BASE_DOCKER_BINARY_ARGS = 10;
    private static final int NUM_BASE_SINGULARITY_PYTHON_ARGS = 14;
    private static final int NUM_BASE_SINGULARITY_BINARY_ARGS = 6;
    private static final int PYTHON_PARAMETER_FORMAT_LENGTH = 6;

    private final ContainerDescription container;
    private final ContainerExecutionType internalExecutionType;
    private final String internalBinary;
    private final String internalFunction;

    private final String workingDir;
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

        this.workingDir = containerImpl.getWorkingDir(); // TODO: Check if this is required
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
        // Command similar to
        // docker exec -it -w X:X ./exec args

        // Get python interpreter and required directories
        String pythonInterpreter = null;
        String pythonPath = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.getPythonInterpreter();
            pythonPath = pp.getPythonPath();
        }

        String workingDir = null;
        workingDir = this.taskSandboxWorkingDir + "/../..";
        if (workingDir.contains("sandBox")) {
            workingDir = this.taskSandboxWorkingDir + "/../..";
        } else {
            workingDir = this.taskSandboxWorkingDir + "";
        }

        String appDir = null;
        appDir = this.context.getAppDir();

        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("[CONTAINER INVOKER] appDir: " + appDir);
        }

        String pyCompssDir = null;
        pyCompssDir = this.context.getInstallDir() + "Bindings/python/" + pythonInterpreter + "/pycompss";

        // Convert binary parameters and calculate binary-streams redirection - binary execution
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        // Prepare command - Determine length of the command
        int numCmdArgs = 0;
        switch (this.container.getEngine()) {
            case DOCKER:
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        numCmdArgs = NUM_BASE_DOCKER_PYTHON_ARGS
                            + this.invocation.getParams().size() * PYTHON_PARAMETER_FORMAT_LENGTH;
                        break;
                    case CET_BINARY:
                        numCmdArgs = NUM_BASE_DOCKER_BINARY_ARGS + binaryParams.size();
                }
                break;
            case SINGULARITY:
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        numCmdArgs = NUM_BASE_SINGULARITY_PYTHON_ARGS
                            + this.invocation.getParams().size() * PYTHON_PARAMETER_FORMAT_LENGTH;
                        break;
                    case CET_BINARY:
                        numCmdArgs = NUM_BASE_SINGULARITY_BINARY_ARGS + binaryParams.size();
                }
        }

        String[] cmd = null;
        cmd = new String[numCmdArgs];
        int cmdIndex = 0;

        // Prepare command - Determine base of the command and container binds
        switch (this.container.getEngine()) {
            case DOCKER:
                cmd[cmdIndex++] = "docker";
                cmd[cmdIndex++] = "run";
                cmd[cmdIndex++] = "-i";
                cmd[cmdIndex++] = "--rm";

                cmd[cmdIndex++] = "-v";
                cmd[cmdIndex++] = workingDir + ":" + workingDir;

                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        cmd[cmdIndex++] = "-v";
                        cmd[cmdIndex++] = appDir + ":" + appDir;
                        cmd[cmdIndex++] = "-v";
                        cmd[cmdIndex++] = pyCompssDir + ":" + pyCompssDir;
                        cmd[cmdIndex++] = "--env";
                        cmd[cmdIndex++] = "PYTHONPATH=" + pythonPath + ":" + pyCompssDir;
                        cmd[cmdIndex++] = "-w";
                        cmd[cmdIndex++] = pyCompssDir + "/";
                        cmd[cmdIndex++] = this.container.getImage();
                        cmd[cmdIndex++] = "python";
                        cmd[cmdIndex++] = "worker/container/container_worker.py";

                        break;
                    case CET_BINARY:
                        cmd[cmdIndex++] = "-w";
                        cmd[cmdIndex++] = this.taskSandboxWorkingDir + "/";
                        cmd[cmdIndex++] = this.container.getImage();
                        cmd[cmdIndex++] = this.internalBinary;
                }
                break;
            case SINGULARITY:
                // TODO: Add pythonpath enviroment variable
                // TODO: Set working dir??

                cmd[cmdIndex++] = "singularity";
                cmd[cmdIndex++] = "exec";

                cmd[cmdIndex++] = "--bind";
                cmd[cmdIndex++] = this.taskSandboxWorkingDir + ":" + this.taskSandboxWorkingDir;
                switch (this.internalExecutionType) {
                    case CET_PYTHON:
                        cmd[cmdIndex++] = "--bind";
                        cmd[cmdIndex++] = appDir + ":" + appDir;
                        cmd[cmdIndex++] = "--bind";
                        cmd[cmdIndex++] = pyCompssDir + ":" + pyCompssDir;
                        cmd[cmdIndex++] = this.container.getImage();
                        cmd[cmdIndex++] = "python";
                        cmd[cmdIndex++] = pyCompssDir + "worker/container/container_worker.py";
                        break;
                    case CET_BINARY:
                        cmd[cmdIndex++] = this.container.getImage();
                        cmd[cmdIndex++] = this.internalBinary;
                }
        }

        // Prepare command - Determine base python arguments
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                String[] parts = this.internalFunction.split("&");
                String userModule = parts[0];
                String userFunction = parts[1];
                cmd[cmdIndex++] = userModule;
                cmd[cmdIndex++] = userFunction;
                cmd[cmdIndex++] = String.valueOf(this.invocation.getParams().size());
                break;
            case CET_BINARY:
        }

        // Prepare command - Prepare user arguments
        switch (this.internalExecutionType) {
            case CET_PYTHON:
                for (int i = 0; i < this.invocation.getParams().size(); ++i) {
                    InvocationParam userParam = this.invocation.getParams().get(i);
                    cmd[cmdIndex++] = String.valueOf(userParam.getType());
                    cmd[cmdIndex++] = String.valueOf(userParam.getStdIOStream());
                    cmd[cmdIndex++] = userParam.getPrefix();
                    cmd[cmdIndex++] = userParam.getName();
                    cmd[cmdIndex++] = null;
                    cmd[cmdIndex++] = String.valueOf(userParam.getValue());
                }
                break;
            case CET_BINARY:
                for (int i = 0; i < binaryParams.size(); ++i) {
                    cmd[cmdIndex + i] = binaryParams.get(i);
                }
        }

        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println("[CONTAINER INVOKER] Begin binary call to container execution");
            outLog.println("[CONTAINER INVOKER] Engine: " + this.container.getEngine().toString());
            outLog.println("[CONTAINER INVOKER] Image: " + this.container.getImage());
            outLog.println("[CONTAINER INVOKER] Internal Type: " + this.internalExecutionType);
            outLog.println("[CONTAINER INVOKER] Internal Binary: " + this.internalBinary);
            outLog.println("[CONTAINER INVOKER] Internal Function: " + this.internalFunction);
            outLog.println("[CONTAINER INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());
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
