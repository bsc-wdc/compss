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
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;

import java.io.PrintStream;
import java.util.ArrayList;


public class OmpSsInvoker extends Invoker {

    private static final int NUM_BASE_OMPSS_ARGS = 1;

    private final String ompssBinary;
    private final boolean failByEV;

    private BinaryRunner br;


    /**
     * OmpSs Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandBox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the COMPSs invoker.
     */
    public OmpSsInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandBox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandBox, assignedResources);

        // Get method definition properties
        OmpSsDefinition ompssImpl = null;
        try {
            ompssImpl = (OmpSsDefinition) this.invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        this.ompssBinary = ompssImpl.getBinary();
        this.failByEV = ompssImpl.isFailByEV();

        // Internal binary runner
        this.br = null;
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.ompssBinary + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.getPythonInterpreter();
        }

        // Close out streams if any
        try {
            if (this.br != null) {
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
        String[] cmd = new String[NUM_BASE_OMPSS_ARGS + binaryParams.size()];
        cmd[0] = this.ompssBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_OMPSS_ARGS + i] = binaryParams.get(i);
        }

        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[OMPSS INVOKER] Begin ompss call to " + this.ompssBinary);
            outLog.println("[OMPSS INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());

            // Debug command
            outLog.print("[OMPSS INVOKER] BINARY CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[OMPSS INVOKER] OmpSs STDIN: " + streamValues.getStdIn());
            outLog.println("[OMPSS INVOKER] OmpSs STDOUT: " + streamValues.getStdOut());
            outLog.println("[OMPSS INVOKER] OmpSs STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.failByEV);
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling OmpSs process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
