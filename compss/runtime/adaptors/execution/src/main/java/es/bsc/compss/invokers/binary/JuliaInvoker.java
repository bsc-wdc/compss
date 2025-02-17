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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.JuliaDefinition;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;


public class JuliaInvoker extends Invoker {

    private static final int NUM_BASE_JULIA_ARGS = 9;

    private static final String ERROR_TARGET_PARAM = "ERROR: Julia execution doesn't support target parameters";

    JuliaDefinition juliaDef;

    private BinaryRunner br;


    /**
     * Julia Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the Julia invoker.
     */
    public JuliaInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);

        // Get method definition properties
        try {
            this.juliaDef = (JuliaDefinition) invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }

        // Internal binary runner
        this.br = null;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.juliaDef.getJuliaScript() + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                // Python interpreter for direct access on stream property calls
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

    private void checkArguments() throws JobExecutionException {
        try {
            juliaDef.checkArguments();
        } catch (IllegalArgumentException ie) {
            throw new JobExecutionException(ie);
        }
        String dfScript = this.juliaDef.getJuliaScript();
        if (!dfScript.startsWith(File.separator)) {
            this.juliaDef.setJuliaScript(context.getAppDir() + File.separator + dfScript);
        }

        if (invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        // Command similar to: exec args
        // e.g. julia juliaScript.jl [args]

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.getPythonInterpreter();
        }

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        // Prepare command
        String args = new String();
        for (int i = 0; i < binaryParams.size(); ++i) {
            if (i == 0) {
                args = args.concat(binaryParams.get(i));
            } else {
                args = args.concat(" " + binaryParams.get(i));
            }
        }
        String[] cmd;
        if (args.isEmpty()) {
            cmd = new String[NUM_BASE_JULIA_ARGS - 1];
        } else {
            cmd = new String[NUM_BASE_JULIA_ARGS];
        }
        final String juliaRunner = this.context.getInstallDir() + JuliaDefinition.SCRIPT_PATH;
        cmd[0] = juliaRunner;
        cmd[1] = this.juliaDef.getJuliaExecutor();
        cmd[2] = this.juliaDef.getJuliaArgs();
        cmd[3] = this.juliaDef.getJuliaScript();
        cmd[4] = this.juliaDef.getWorkingDir();
        cmd[5] = String.valueOf(this.juliaDef.getFailByEV());
        cmd[6] = String.valueOf(this.juliaDef.getComputingNodes());
        cmd[7] = String.valueOf(this.invocation.isDebugEnabled());

        if (!args.isEmpty()) {
            cmd[7] = args;
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[JULIA INVOKER] Begin JULIA with args: " + this.juliaDef.getJuliaArgs());
            outLog.println("[JULIA INVOKER] For script : " + this.juliaDef.getJuliaScript());
            outLog.println("[JULIA INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[JULIA INVOKER] Julia CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[JULIA INVOKER] Julia STDIN: " + streamValues.getStdIn());
            outLog.println("[JULIA INVOKER] Julia STDOUT: " + streamValues.getStdOut());
            outLog.println("[JULIA INVOKER] Julia STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.juliaDef.getFailByEV());
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling Julia process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
