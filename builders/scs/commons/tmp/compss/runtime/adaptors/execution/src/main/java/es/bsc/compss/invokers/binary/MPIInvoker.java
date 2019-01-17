/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.BinaryRunner.StreamSTD;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.MPIImplementation;
import java.io.PrintStream;
import java.util.ArrayList;


public class MPIInvoker extends Invoker {

    private static final int NUM_BASE_MPI_ARGS = 6;

    private static final String ERROR_MPI_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_MPI_BINARY = "ERROR: Invalid mpiBinary";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private final String mpiBinary;


    public MPIInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources)
            throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        // Get method definition properties
        MPIImplementation mpiImpl = null;
        try {
            mpiImpl = (MPIImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // MPI flags
        this.mpiRunner = mpiImpl.getMpiRunner();
        this.mpiBinary = mpiImpl.getBinary();
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
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
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
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args
        // Get COMPSS ENV VARS
        String numProcs = String.valueOf(this.numWorkers * this.computingUnits);

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(invocation.getParams(), invocation.getTarget(),
                streamValues);

        // Create hostfile
        String hostfile = writeHostfile(taskSandboxWorkingDir, workers);

        // Prepare command
        String[] cmd = new String[NUM_BASE_MPI_ARGS + binaryParams.size()];
        cmd[0] = this.mpiRunner;
        cmd[1] = "-hostfile";
        cmd[2] = hostfile;
        cmd[3] = "-n";
        cmd[4] = numProcs;
        // cmd[5] = "--bind-to";
        // cmd[6] = "core";
        cmd[5] = this.mpiBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_MPI_ARGS + i] = binaryParams.get(i);
        }

        // Prepare environment
        if (invocation.isDebugEnabled()) {
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
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(),
                context.getThreadErrStream());
    }
}
