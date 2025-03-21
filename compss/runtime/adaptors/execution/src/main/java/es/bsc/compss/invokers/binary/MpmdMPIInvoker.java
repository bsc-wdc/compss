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
import es.bsc.compss.types.MPIProgram;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.implementations.definition.MpmdMPIDefinition;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;


public class MpmdMPIInvoker extends Invoker {

    private static final int NUM_BASE_MPI_ARGS = 6;

    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private MpmdMPIDefinition definition;

    private BinaryRunner br;


    /**
     * MPMDMPI Invoker constructor.
     *
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the MPI invoker.
     */
    public MpmdMPIInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandbox, assignedResources);

        // Get method definition properties
        try {
            this.definition = (MpmdMPIDefinition) this.invocation.getMethodImplementation().getDefinition();
            this.definition.setRunnerProperties(context.getInstallDir());
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // Internal binary runner
        this.br = null;
    }

    private void checkArguments() throws JobExecutionException {
        try {
            definition.checkArguments();
        } catch (IllegalArgumentException e) {
            throw new JobExecutionException(e);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoking MPMDMPI " + this.definition + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException | IOException iee) {
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

    private Object runInvocation() throws InvokeExecutionException, IOException {

        // update container options for each program before building the command
        ContainerDescription container = this.definition.getContainer();
        if (container != null) {
            String optionsStr = container.getOptions();
            String[] options = BinaryRunner.buildAppParams(this.invocation.getParams(), optionsStr, null);
            StringBuilder stringBuilder = new StringBuilder();
            for (String option : options) {
                stringBuilder.append(option);
            }
            container.setOptions(stringBuilder.toString());
        }

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.checkCoverageAndGetPythonInterpreter();

        }

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        // Update params string for each program
        for (MPIProgram program : this.definition.getPrograms()) {
            if (program.hasParamsString()) {
                String[] tmp =
                    BinaryRunner.buildAppParams(this.invocation.getParams(), program.getParams(), pythonInterpreter);
                program.setParams(String.join(" ", tmp));
                program.setParamsArray(tmp);
            }
        }

        String[] cmd =
            this.definition.generateCMD(this.sandBox.getFolder(), this.hostnames, this.computingUnits, container);
        // Launch command
        this.br = new BinaryRunner();

        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println("[MPMD MPI INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[MPMD MPI INVOKER] BINARY CMD: ");
            for (String s : cmd) {
                outLog.print(s + " ");
            }
            outLog.println("");
            outLog.println("[MPMD MPI INVOKER] Binary STDIN: " + streamValues.getStdIn());
            outLog.println("[MPMD MPI INVOKER] Binary STDOUT: " + streamValues.getStdOut());
            outLog.println("[MPMD MPI INVOKER] Binary STDERR: " + streamValues.getStdErr());
        }

        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.definition.isFailByEV());
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling MPI process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }

    @Override
    protected void setEnvironmentVariables() {
        super.setEnvironmentVariables();
        int ppn = definition.getPPN();
        if (LOGGER.isDebugEnabled()) {
            System.out.println("[MPMD MPI INVOKER] OVERWRITING COMPSS_NUM_PROCS: " + this.computingUnits);
        }
        System.setProperty(COMPSS_NUM_PROCS, String.valueOf(this.computingUnits));

        if (ppn > 1) {
            int threads = this.computingUnits / ppn;
            System.setProperty(COMPSS_NUM_THREADS, String.valueOf(threads));
            System.setProperty(OMP_NUM_THREADS, String.valueOf(threads));

            // LOG ENV VARS
            if (LOGGER.isDebugEnabled()) {
                System.out.println("[MPMD MPI INVOKER] OVERWRITING COMPSS_NUM_THREADS: " + threads);
            }
        }
    }

}
