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
package es.bsc.compss.invokers.external;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.executor.external.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.StdIOStream;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.CollectionLayout;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;


public class PythonMPIInvoker extends ExternalInvoker {

    private static final int NUM_BASE_PYTHON_MPI_ARGS = 8;

    private final PythonMPIDefinition mpiDef;
    private BinaryRunner br;


    /**
     * Python MPI Invoker constructor.
     *
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandbox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the MPI invoker.
     */
    public PythonMPIInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, sandbox, assignedResources);
        try {
            this.mpiDef = (PythonMPIDefinition) invocation.getMethodImplementation().getDefinition();
            this.mpiDef.setRunnerProperties(context.getInstallDir());
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }
        super.appendOtherExecutionCommandArguments();

        // Internal binary runner
        this.br = null;

    }

    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {

        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId(), sandBox);
        return taskExecution;
    }

    @Override
    protected void invokeExternalMethod() throws JobExecutionException {
        try {
            mpiDef.checkArguments();
        } catch (IllegalArgumentException e) {
            throw new JobExecutionException(e);
        }
        Object retObj;
        try {
            retObj = runPythonMPIInvocation();
        } catch (InvokeExecutionException e1) {
            throw new JobExecutionException(e1);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                // Python interpreter for direct access on stream property calls
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

        if (this.invocation.isDebugEnabled()) {
            LOGGER.debug("Exit value of MPI executor of job " + this.invocation.getJobId() + " of task "
                + this.invocation.getTaskId() + ": " + retObj.toString());
        }

        if (retObj.toString().compareTo("0") != 0) {
            throw new JobExecutionException("Received non-zero exit value: " + retObj.toString() + " for job "
                + this.invocation.getJobId() + " of task " + this.invocation.getTaskId());
        }
    }

    private Object runPythonMPIInvocation() throws InvokeExecutionException {
        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args
        // Get COMPSS ENV VARS

        final String taskCMD = this.command.getAsString();

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.checkCoverageAndGetPythonInterpreter();
        } else {
            LOGGER.error("Incorrect language parameters for PYTHON MPI task. No Python language parameters");
            throw new InvokeExecutionException(
                "Incorrect language parameters for PYTHON MPI task. No Python language parameters");
        }

        // MPI Flags
        int numMPIFlags = 0;
        String mpiFlags = this.mpiDef.getMpiFlags();
        String[] mpiflagsArray = null;
        if (mpiFlags == null || mpiFlags.isEmpty()) {
            mpiflagsArray = mpiFlags.split(" ");
            numMPIFlags = mpiflagsArray.length;
        }

        String[] pythonInterpreterArray = null;
        int interpreterflags = 0;
        if (pythonInterpreter != null) {
            pythonInterpreterArray = pythonInterpreter.split(" ");
            interpreterflags = pythonInterpreterArray.length - 1;
        }

        int numBasePythonMpiArgs = NUM_BASE_PYTHON_MPI_ARGS;
        numBasePythonMpiArgs = numBasePythonMpiArgs + numMPIFlags + interpreterflags;

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        // Prepare command
        String[] cmd = new String[numBasePythonMpiArgs + binaryParams.size()];
        cmd[0] = this.mpiDef.getMpiRunner();
        cmd[1] = this.mpiDef.getHostsFlag();
        try {
            cmd[2] = this.mpiDef.generateHostsDefinition(this.sandBox.getFolder(), this.hostnames, this.computingUnits);
        } catch (IOException ioe) {
            throw new InvokeExecutionException("ERROR: writting hostfile", ioe);
        }
        cmd[3] = "-n";
        cmd[4] = this.mpiDef.generateNumberOfProcesses(this.numWorkers, this.computingUnits);
        // cmd[5] = "-x";
        // cmd[6] = "OMP_NUM_THREADS";

        // Add mpiFlags
        for (int i = 0; i < numMPIFlags; ++i) {
            cmd[5 + i] = mpiflagsArray[i];
        }
        // adding python interpreter (including when coverage is set)
        if (pythonInterpreterArray != null || pythonInterpreterArray.length > 0) {
            for (int i = 0; i < pythonInterpreterArray.length; i++) {
                cmd[numBasePythonMpiArgs - (2 + pythonInterpreterArray.length - i)] = pythonInterpreterArray[i];
            }
        } else {
            LOGGER.error("Incorrect python interpreter parameter");
            throw new InvokeExecutionException("Incorrect python interpreter parameter");
        }
        String installDir = this.context.getInstallDir();
        final String pycompssRelativePath = File.separator + "Bindings" + File.separator + "python";
        String pythonVersion =
            ((PythonParams) this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON)).getPythonVersion();
        String pyCOMPSsHome = installDir + pycompssRelativePath + File.separator + pythonVersion;

        cmd[numBasePythonMpiArgs - 2] = pyCOMPSsHome + File.separator + "pycompss" + File.separator + "worker"
            + File.separator + "external" + File.separator + "mpi_executor.py";

        CollectionLayout[] cls = mpiDef.getCollectionLayouts();
        int collectionLayoutNum = cls == null ? 0 : cls.length;
        StringBuilder collectionLayoutParams = new StringBuilder(" ");

        for (CollectionLayout cl : cls) {
            collectionLayoutParams.append(cl.getParamName()).append(" ");
            collectionLayoutParams.append(Integer.toString(cl.getBlockCount())).append(" ");
            collectionLayoutParams.append(Integer.toString(cl.getBlockLen())).append(" ");
            collectionLayoutParams.append(Integer.toString(cl.getBlockStride())).append(" ");
        }

        collectionLayoutParams.append(Integer.toString(collectionLayoutNum));

        cmd[numBasePythonMpiArgs - 1] = taskCMD + collectionLayoutParams.toString();

        String pythonPath = System.getenv("PYTHONPATH");
        pythonPath = pyCOMPSsHome + ":" + pythonPath;

        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[numBasePythonMpiArgs + i] = binaryParams.get(i);
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println("[Python MPI INVOKER] Begin MPI call to " + this.mpiDef.getDeclaringClass() + "."
                + this.mpiDef.getAlternativeMethodName());
            outLog.println("[Python MPI INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[Python MPI INVOKER] MPI CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
        }

        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), pythonPath, this.mpiDef.isFailByEV());
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling PythonMPI process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }

    @Override
    protected int getNumThreads(InvocationContext context, Invocation invocation) {
        int ppn = this.mpiDef.getPPN();
        if (ppn > 1) {
            return this.computingUnits / ppn;
        } else {
            return this.computingUnits;
        }
    }

    @Override
    protected void setEnvironmentVariables() {
        super.setEnvironmentVariables();
        int ppn = this.mpiDef.getPPN();
        if (LOGGER.isDebugEnabled()) {
            System.out.println("[PYTHON MPI INVOKER] OVERWRITING COMPSS_NUM_PROCS: " + this.computingUnits);
        }
        System.setProperty(COMPSS_NUM_PROCS, String.valueOf(this.computingUnits));
        if (ppn > 1) {
            int threads = this.computingUnits / ppn;
            System.setProperty(COMPSS_NUM_THREADS, String.valueOf(threads));
            System.setProperty(OMP_NUM_THREADS, String.valueOf(threads));

            // LOG ENV VARS
            if (LOGGER.isDebugEnabled()) {
                System.out.println("[INVOKER] OVEWRITING COMPSS_NUM_THREADS: " + threads);
            }
        }
    }
}
