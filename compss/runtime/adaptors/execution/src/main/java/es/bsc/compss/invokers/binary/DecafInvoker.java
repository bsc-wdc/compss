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
import es.bsc.compss.types.implementations.definition.DecafDefinition;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;


public class DecafInvoker extends Invoker {

    private static final int NUM_BASE_DECAF_ARGS = 11;

    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    DecafDefinition decafDef;

    private BinaryRunner br;


    /**
     * Decaf Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the Decaf invoker.
     */
    public DecafInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);

        // Get method definition properties
        try {
            this.decafDef = (DecafDefinition) invocation.getMethodImplementation().getDefinition();
            this.decafDef.setRunnerProperties(context.getInstallDir());
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

        LOGGER.info("Invoked " + this.decafDef.getDfScript() + " in " + this.context.getHostName());

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
            decafDef.checkArguments();
        } catch (IllegalArgumentException ie) {
            throw new JobExecutionException(ie);
        }
        String dfScript = this.decafDef.getDfScript();
        if (!dfScript.startsWith(File.separator)) {
            this.decafDef.setDfScript(context.getAppDir() + File.separator + dfScript);
        }

        if (invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args

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
        String args = "";
        for (int i = 0; i < binaryParams.size(); ++i) {
            if (i == 0) {
                args = args.concat(binaryParams.get(i));
            } else {
                args = args.concat(" " + binaryParams.get(i));
            }
        }
        String[] cmd;
        if (args.isEmpty()) {
            cmd = new String[NUM_BASE_DECAF_ARGS - 2];
        } else {
            cmd = new String[NUM_BASE_DECAF_ARGS - 1 + binaryParams.size()];
        }
        final String dfRunner = this.context.getInstallDir() + DecafDefinition.SCRIPT_PATH;
        cmd[0] = dfRunner;
        cmd[1] = this.decafDef.getDfScript();
        cmd[2] = this.decafDef.getDfExecutor();
        cmd[3] = this.decafDef.getDfLib();
        cmd[4] = this.decafDef.getMpiRunner();

        String numProcs = String.valueOf(this.numWorkers * this.computingUnits);
        cmd[5] = "-n";
        cmd[6] = numProcs;
        cmd[7] = this.decafDef.getHostsFlag();
        try {
            cmd[8] =
                this.decafDef.generateHostsDefinition(this.sandBox.getFolder(), this.hostnames, this.computingUnits);
        } catch (IOException ioe) {
            throw new InvokeExecutionException("ERROR: writting hostfile", ioe);
        }
        if (!args.isEmpty()) {
            cmd[9] = "--args";
            for (int i = 0; i < binaryParams.size(); ++i) {
                cmd[10 + i] = binaryParams.get(i);
            }
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[DECAF INVOKER] Begin DECAF call to " + this.decafDef.getDfScript());
            outLog.println("[DECAF INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[DECAF INVOKER] Decaf CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[DECAF INVOKER] Decaf STDIN: " + streamValues.getStdIn());
            outLog.println("[DECAF INVOKER] Decaf STDOUT: " + streamValues.getStdOut());
            outLog.println("[DECAF INVOKER] Decaf STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.decafDef.isFailByEV());
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling Decaf process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
