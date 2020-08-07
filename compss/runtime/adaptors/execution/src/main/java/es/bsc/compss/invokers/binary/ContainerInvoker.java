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

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.StdIOStream;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.resources.ContainerDescription;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;


public class ContainerInvoker extends Invoker {

    private static final int NUM_BASE_DOCKER_ARGS = 10;
    private static final int NUM_BASE_SINGULARITY_ARGS = 6;
    private static final String DOCKER_ENGINE = "DOCKER";
    private static final String SINGULARITY_ENGINE = "SINGULARITY";

    private final String binary;
    private final boolean failByEV;
    private final ContainerDescription container;

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

        this.binary = containerImpl.getBinary();
        this.failByEV = containerImpl.isFailByEV();

        // Internal binary runner
        this.br = null;

        this.container = containerImpl.getContainer();
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.binary + " in " + this.context.getHostName());

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
                this.br.closeStreams(this.invocation.getParams(), this.pythonInterpreter);
            }
        } catch (StreamCloseException se) {
            LOGGER.error("Exception closing container streams", se);
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
        // ./exec args
        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, this.pythonInterpreter);

        // Prepare command
        String[] cmd = null;
        switch (container.getEngine()) {
            case DOCKER_ENGINE:
                cmd = new String[NUM_BASE_DOCKER_ARGS + binaryParams.size()];
                cmd[0] = "docker";
                cmd[1] = "run";
                cmd[2] = "-i";
                cmd[3] = "--rm";
                cmd[4] = "-v";
                cmd[5] = this.taskSandboxWorkingDir + "/../..";
                if (cmd[5].contains("sandBox")) {
                    cmd[5] = this.taskSandboxWorkingDir + "/../.." + ":" + this.taskSandboxWorkingDir + "/../..";
                } else {
                    cmd[5] = this.taskSandboxWorkingDir + ":" + this.taskSandboxWorkingDir;
                }
                cmd[6] = "-w";
                cmd[7] = this.taskSandboxWorkingDir + "/";
                cmd[8] = container.getImage();
                cmd[9] = this.binary;
                for (int i = 0; i < binaryParams.size(); ++i) {
                    cmd[NUM_BASE_DOCKER_ARGS + i] = binaryParams.get(i);
                }
                break;
            case SINGULARITY_ENGINE:
                cmd = new String[NUM_BASE_SINGULARITY_ARGS + binaryParams.size()];
                cmd[0] = "singularity";
                cmd[1] = "exec";
                cmd[2] = "--bind";
                cmd[3] = this.taskSandboxWorkingDir + ":" + this.taskSandboxWorkingDir;
                cmd[4] = container.getImage();
                cmd[5] = this.binary;
                for (int i = 0; i < binaryParams.size(); ++i) {
                    cmd[NUM_BASE_SINGULARITY_ARGS + i] = binaryParams.get(i);
                }
                break;
            default:
                throw new InvokeExecutionException("Invalid engine name");
        }

        if (invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[CONTAINER INVOKER] Begin binary call to " + this.binary);
            outLog.println("[CONTAINER INVOKER] " + this.container);
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
