/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.MPIDefinition;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;


public class MPIInvoker extends Invoker {

    private static final int NUM_BASE_MPI_ARGS = 6;

    private static final String ERROR_MPI_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_MPI_BINARY = "ERROR: Invalid mpiBinary";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private final String mpiFlags;
    private final String mpiBinary;
    private final boolean scaleByCU;
    private final boolean failByEV;

    private BinaryRunner br;


    /**
     * MPI Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param taskSandboxWorkingDir Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the MPI invoker.
     */
    public MPIInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        MPIDefinition mpiImpl = null;
        try {
            mpiImpl = (MPIDefinition) this.invocation.getMethodImplementation().getDefinition();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // MPI flags
        this.mpiRunner = mpiImpl.getMpiRunner();
        this.mpiFlags = mpiImpl.getMpiFlags();
        this.mpiBinary = mpiImpl.getBinary();
        this.scaleByCU = mpiImpl.getScaleByCU();
        this.failByEV = mpiImpl.isFailByEV();

        // Internal binary runner
        this.br = null;
    }

    private void checkArguments() throws JobExecutionException {
        if (this.mpiRunner == null || this.mpiRunner.isEmpty()) {
            throw new JobExecutionException(ERROR_MPI_RUNNER);
        }
        if (this.mpiBinary == null || this.mpiBinary.isEmpty()) {
            throw new JobExecutionException(ERROR_MPI_BINARY);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.mpiBinary + " in " + this.context.getHostName());

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

    private Object runInvocation() throws InvokeExecutionException {
        // Command similar to
        // mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 (--bind-to core) exec args

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

        // Create hostfile
        String hostfile = null;
        int numMPIArgs = NUM_BASE_MPI_ARGS;
        if (this.scaleByCU) {
            hostfile = writeHostfile(this.taskSandboxWorkingDir, this.workers);
        } else {
            hostfile = writeHostfile(this.taskSandboxWorkingDir, this.hostnames);
            // numMPIArgs = numMPIArgs + 2; // to add the -x OMP_NUM_THREADS
        }

        // MPI Flags
        int numMPIFlags = 0;
        String[] mpiflagsArray = null;
        if (this.mpiFlags != null && !this.mpiFlags.isEmpty() && !this.mpiFlags.equals("[unassigned]")) {
            mpiflagsArray = this.mpiFlags.split(" ");
            numMPIFlags = mpiflagsArray.length;
        }

        // Prepare command
        String[] cmd = new String[numMPIArgs + numMPIFlags + binaryParams.size()];
        int pos = 0;
        cmd[pos++] = this.mpiRunner;
        cmd[pos++] = "-hostfile";
        cmd[pos++] = hostfile;
        cmd[pos++] = "-n";
        if (this.scaleByCU) {
            cmd[pos++] = String.valueOf(this.numWorkers * this.computingUnits);
        } else {
            cmd[pos++] = String.valueOf(this.numWorkers);
        }

        for (int i = 0; i < numMPIFlags; ++i) {
            cmd[pos++] = mpiflagsArray[i];
        }

        cmd[pos++] = this.mpiBinary;

        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[pos++] = binaryParams.get(i);
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[MPI INVOKER] Begin MPI call to " + this.mpiBinary);
            outLog.println("[MPI INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());
            // Debug command
            outLog.print("[MPI INVOKER] MPI CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[MPI INVOKER] MPI STDIN: " + streamValues.getStdIn());
            outLog.println("[MPI INVOKER] MPI STDOUT: " + streamValues.getStdOut());
            outLog.println("[MPI INVOKER] MPI STDERR: " + streamValues.getStdErr());
        }

        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), null, this.failByEV);
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling MPI process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
