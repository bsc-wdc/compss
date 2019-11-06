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
package es.bsc.compss.invokers.external;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.executor.external.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.StdIOStream;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.PythonMPIImplementation;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;


public class PythonMPIInvoker extends ExternalInvoker {

    private static final int NUM_BASE_PYTHON_MPI_ARGS = 8;

    private final String mpiRunner;
    private final String mpiFlags;
    private final String declaringclass;
    private final String alternativeMethod;
    private final boolean scaleByCU;
    private final boolean failByEV;

    private BinaryRunner br;


    public class CollectionLayout {

        String paramName;
        int blockCount;
        int blockLen;
        int blockStride;
    }


    private CollectionLayout cl;


    /**
     * Python MPI Invoker constructor.
     *
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param taskSandboxWorkingDir Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the MPI invoker.
     */
    public PythonMPIInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        PythonMPIImplementation pythonmpiImpl = null;
        try {
            pythonmpiImpl = (PythonMPIImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // Python MPI flags
        this.mpiRunner = pythonmpiImpl.getMpiRunner();
        this.mpiFlags = pythonmpiImpl.getMpiFlags();
        this.declaringclass = pythonmpiImpl.getDeclaringClass();
        this.alternativeMethod = pythonmpiImpl.getAlternativeMethodName();
        this.scaleByCU = pythonmpiImpl.getScaleByCU();
        this.failByEV = pythonmpiImpl.isFailByEV();

        // Internal binary runner
        this.br = null;
        this.cl = new CollectionLayout();
        this.cl.paramName = pythonmpiImpl.getCollectionLayout().getParamName();
        this.cl.blockCount = pythonmpiImpl.getCollectionLayout().getBlockCount();
        this.cl.blockLen = pythonmpiImpl.getCollectionLayout().getBlockLen();
        this.cl.blockStride = pythonmpiImpl.getCollectionLayout().getBlockStride();

        System.out.println(">>>>>> " + this.cl.paramName);
        System.out.println(">>>>>> " + this.cl.blockCount);
        System.out.println(">>>>>> " + this.cl.blockLen);
        System.out.println(">>>>>> " + this.cl.blockStride);

        if ((this.cl.blockCount + this.cl.blockLen + this.cl.blockStride) < 0) {
            this.cl = null;
        }

    }

    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {

        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId());
        return taskExecution;
    }

    @Override
    protected void invokeMethod() throws JobExecutionException {

        Object retObj;
        try {
            retObj = runPythonMPIInvocation();
        } catch (InvokeExecutionException e1) {
            throw new JobExecutionException(e1);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                this.br.closeStreams(this.invocation.getParams(), this.pythonInterpreter);
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
        int numBasePythonMpiArgs = NUM_BASE_PYTHON_MPI_ARGS;
        String hostfile = null;
        if (this.scaleByCU) {
            hostfile = writeHostfile(taskSandboxWorkingDir, workers);
        } else {
            hostfile = writeHostfile(taskSandboxWorkingDir, hostnames);
            // numBasePythonMpiArgs = numBasePythonMpiArgs + 2; // to add the -x OMP_NUM_THREADS
        }

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, this.pythonInterpreter);

        // MPI Flags
        int numMPIFlags = 0;
        String[] mpiflagsArray = null;
        if (this.mpiFlags == null || this.mpiFlags.isEmpty()) {
            mpiflagsArray = mpiFlags.split(" ");
            numMPIFlags = mpiflagsArray.length;
        }

        numBasePythonMpiArgs = numBasePythonMpiArgs + numMPIFlags;

        // Prepare command
        String[] cmd = new String[numBasePythonMpiArgs + binaryParams.size()];
        cmd[0] = this.mpiRunner;
        cmd[1] = "-hostfile";
        cmd[2] = hostfile;
        cmd[3] = "-n";
        if (scaleByCU) {
            cmd[4] = String.valueOf(this.numWorkers * this.computingUnits);
        } else {
            cmd[4] = String.valueOf(this.numWorkers);
            // cmd[5] = "-x";
            // cmd[6] = "OMP_NUM_THREADS";
        }

        // Add mpiFlags
        for (int i = 0; i < numMPIFlags; ++i) {
            cmd[5 + i] = mpiflagsArray[i];
        }

        cmd[numBasePythonMpiArgs - 3] =
            ((PythonParams) this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON)).getPythonInterpreter();
        String installDir = this.context.getInstallDir();
        final String pycompssRelativePath = File.separator + "Bindings" + File.separator + "python";
        String pythonVersion =
            ((PythonParams) this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON)).getPythonVersion();
        String pyCOMPSsHome = installDir + pycompssRelativePath + File.separator + pythonVersion;

        cmd[numBasePythonMpiArgs - 2] = pyCOMPSsHome + File.separator + "pycompss" + File.separator + "worker"
            + File.separator + "external" + File.separator + "mpi_executor.py";

        //TODO, to be review this could not work
	//OLD part:
	//cmd[numBasePythonMpiArgs - 1] = taskCMD;
	int collectionLayoutNum = this.cl == null ? 0 : 1;
        int collectionLayoutParamsNum = collectionLayoutNum * 4;
        String collectionLayoutParams = " ";

        for (int i = 0; i < collectionLayoutParamsNum; i += 4) {
            collectionLayoutParams += this.cl.paramName + " ";
            collectionLayoutParams += Integer.toString(this.cl.blockCount) + " ";
            collectionLayoutParams += Integer.toString(this.cl.blockLen) + " ";
            collectionLayoutParams += Integer.toString(this.cl.blockStride) + " ";
        }

        collectionLayoutParams += Integer.toString(collectionLayoutNum);
	
	//Merged
        cmd[numBasePythonMpiArgs - 1] = taskCMD + collectionLayoutParams;

        String pythonPath = System.getenv("PYTHONPATH");
        pythonPath = pyCOMPSsHome + ":" + pythonPath;

        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[numBasePythonMpiArgs + i] = binaryParams.get(i);
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = this.context.getThreadOutStream();
            outLog.println("");
            outLog.println(
                "[Python MPI INVOKER] Begin MPI call to " + this.declaringclass + "." + this.alternativeMethod);
            outLog.println("[Python MPI INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());
            // Debug command
            outLog.print("[Python MPI INVOKER] MPI CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
        }

        // Launch command
        this.br = new BinaryRunner();
        return this.br.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, this.context.getThreadOutStream(),
            this.context.getThreadErrStream(), pythonPath, this.failByEV);
    }

    @Override
    public void cancelMethod() {
        LOGGER.debug("Cancelling PythonMPI process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }
}
