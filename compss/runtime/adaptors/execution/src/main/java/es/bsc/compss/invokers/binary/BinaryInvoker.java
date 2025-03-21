/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

import static es.bsc.compss.types.implementations.definition.ContainerDescription.ContainerEngine.SINGULARITY;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDescription;

import java.io.PrintStream;
import java.util.ArrayList;


public class BinaryInvoker extends Invoker {

    private static final int NUM_BASE_BINARY_ARGS = 1;

    private final String binary;
    private final boolean failByEV;

    private BinaryDefinition binDef;
    private BinaryRunner br;


    /**
     * Binary Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the binary invoker.
     */
    public BinaryInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);

        // Get method definition properties
        try {
            this.binDef = (BinaryDefinition) invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }

        this.binary = this.binDef.getBinary();
        this.failByEV = this.binDef.isFailByEV();

        // Internal binary runner
        this.br = null;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.binary + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            LOGGER.error("Exception running binary", iee);
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

        // Update binary results
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

        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.getPythonInterpreter();
        }

        // Calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        String[] appParams = new String[0];
        if (this.binDef.hasParamsString()) {
            appParams =
                BinaryRunner.buildAppParams(this.invocation.getParams(), this.binDef.getParams(), pythonInterpreter);
        }

        // Prepare command
        int cmdLength = NUM_BASE_BINARY_ARGS;
        if (this.binDef.hasParamsString()) {
            cmdLength += appParams.length;
        } else {
            cmdLength += binaryParams.size();
        }

        // Check container and its options
        ContainerDescription container = this.binDef.getContainer();
        int numOptions = 0;
        String[] options = null;
        if (container != null) {
            // engine, exec_command, options[], image
            cmdLength += 3;
            if (!container.getOptions().isEmpty() && !container.getOptions().equals(Constants.UNASSIGNED)) {
                options =
                    BinaryRunner.buildAppParams(this.invocation.getParams(), container.getOptions(), pythonInterpreter);
                numOptions = options.length;
            }
            // -e DOCKER_WORKING_DIR_VOLUME="working_dir" -e DOCKER_WORKING_DIR_MOUNT="/docker_working_dir/"
            String dockerWorkDirVolume = System.getenv(COMPSsConstants.DOCKER_WORKING_DIR_VOLUME);
            if (dockerWorkDirVolume != null && !dockerWorkDirVolume.isEmpty()) {
                numOptions += 4;
            }
            cmdLength += numOptions;
        }

        String[] cmd = new String[cmdLength];

        int pos = 0;

        if (container != null) {
            // mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 <engine> <exec_command> <options> <image> binary args
            cmd[pos++] = container.getEngine().name().toLowerCase();
            cmd[pos++] = container.getEngine().equals(SINGULARITY) ? "exec" : "run";
            // Check options
            pos = ContainerInvoker.addContainerOptions(cmd, pos, options);

            // -e DOCKER_WORKING_DIR_VOLUME="working_dir" -e DOCKER_WORKING_DIR_MOUNT="/docker_working_dir/"
            String dockerWorkDirVolume = System.getenv(COMPSsConstants.DOCKER_WORKING_DIR_VOLUME);
            if (dockerWorkDirVolume != null && !dockerWorkDirVolume.isEmpty()) {
                cmd[pos++] = "-e";
                cmd[pos++] = COMPSsConstants.DOCKER_WORKING_DIR_VOLUME + "=\"" + dockerWorkDirVolume + "\"";
                String dockerWorkDirMount = System.getenv(COMPSsConstants.DOCKER_WORKING_DIR_MOUNT);
                cmd[pos++] = "-e";
                cmd[pos++] = COMPSsConstants.DOCKER_WORKING_DIR_MOUNT + "=\"" + dockerWorkDirMount + "\"";
            }
            cmd[pos++] = container.getImage();
        }

        cmd[pos++] = this.binary;

        // if params string is not null, all the params should be included there
        if (this.binDef.hasParamsString()) {
            for (String appParam : appParams) {
                cmd[pos++] = appParam;
            }
        } else {
            for (String binParam : binaryParams) {
                cmd[pos++] = binParam;
            }
        }

        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println("[BINARY INVOKER] Begin binary call to " + this.binary);
            outLog.println("[BINARY INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[BINARY INVOKER] BINARY CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[BINARY INVOKER] Binary STDIN: " + streamValues.getStdIn());
            outLog.println("[BINARY INVOKER] Binary STDOUT: " + streamValues.getStdOut());
            outLog.println("[BINARY INVOKER] Binary STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
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
