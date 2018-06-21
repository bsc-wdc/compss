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
package es.bsc.compss.invokers;

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.DecafImplementation;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;


public class DecafInvoker extends Invoker {

    private static final int NUM_BASE_DECAF_ARGS = 11;

    private static final String ERROR_DECAF_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_DECAF_BINARY = "ERROR: Invalid wfScript";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private String dfScript;
    private String dfExecutor;
    private String dfLib;

    public DecafInvoker(InvocationContext context, Invocation invocation, boolean debug, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(context, invocation, debug, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        DecafImplementation decafImpl = null;
        try {
            decafImpl = (DecafImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.impl.getMethodType(), e);
        }
        this.mpiRunner = decafImpl.getMpiRunner();
        this.dfScript = decafImpl.getDfScript();
        this.dfExecutor = decafImpl.getDfExecutor();
        this.dfLib = decafImpl.getDfLib();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        checkArguments();
        try {
            LOGGER.info("Invoked " + this.dfScript + " in " + this.context.getHostName());
            return runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

    private void checkArguments() throws JobExecutionException {
        if (this.mpiRunner == null || this.mpiRunner.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_RUNNER);
        }
        if (this.dfScript == null || this.dfScript.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_BINARY);
        }
        if (!this.dfScript.startsWith(File.separator)) {
            this.dfScript = context.getAppDir() + File.separator + this.dfScript;
        }
        if (this.dfExecutor == null || this.dfExecutor.isEmpty() || this.dfExecutor.equals(Constants.UNASSIGNED)) {
            this.dfExecutor = "executor.sh";
        }
        if (!this.dfExecutor.startsWith(File.separator) && !this.dfExecutor.startsWith("./")) {
            this.dfExecutor = "./" + this.dfExecutor;
        }
        if (this.dfLib == null || this.dfLib.isEmpty()) {
            this.dfLib = "null";
        }
        if (this.target.getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        String dfRunner = context.getInstallDir() + DecafImplementation.SCRIPT_PATH;
        System.out.println("");
        System.out.println("[DECAF INVOKER] Begin DECAF call to " + this.dfScript);
        System.out.println("[DECAF INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args
        // Get COMPSS ENV VARS
        String workers = System.getProperty(Constants.COMPSS_HOSTNAMES);
        String numNodes = System.getProperty(Constants.COMPSS_NUM_NODES);
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        String numProcs = String.valueOf(Integer.valueOf(numNodes) * Integer.valueOf(computingUnits));
        System.out.println("[DECAF INVOKER] COMPSS HOSTNAMES: " + workers);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_NODES: " + numNodes);
        System.out.println("[DECAF INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Convert binary parameters and calculate binary-streams redirection
        BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.values, this.streams, this.prefixes, streamValues);
        String hostfile = writeHostfile(this.taskSandboxWorkingDir, workers);
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
            cmd = new String[NUM_BASE_DECAF_ARGS - 2];
        } else {
            cmd = new String[NUM_BASE_DECAF_ARGS];
        }
        cmd[0] = dfRunner;
        cmd[1] = this.dfScript;
        cmd[2] = this.dfExecutor;
        cmd[3] = this.dfLib;
        cmd[4] = this.mpiRunner;
        cmd[5] = "-n";
        cmd[6] = numProcs;
        cmd[7] = "--hostfile";
        cmd[8] = hostfile;
        if (!args.isEmpty()) {
            cmd[9] = "--args=\"";
            cmd[10] = args;
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("[DECAF INVOKER] Decaf CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");
        System.out.println("[DECAF INVOKER] Decaf STDIN: " + streamValues.getStdIn());
        System.out.println("[DECAF INVOKER] Decaf STDOUT: " + streamValues.getStdOut());
        System.out.println("[DECAF INVOKER] Decaf STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(), context.getThreadErrStream());
    }

    private static String writeHostfile(File taskSandboxWorkingDir, String workers) throws InvokeExecutionException {
        String filename = taskSandboxWorkingDir.getAbsolutePath() + File.separator + ".decafHostfile";
        String workersInLines = workers.replace(',', '\n');
        BufferedWriter writer = null;
        try {
<<<<<<< eb4b41f13c640caf41497a8df44988b14bd6c064
            return GenericInvoker.invokeDecafMethod(context.getInstallDir() + DecafImplementation.SCRIPT_PATH, this.dfScript, this.dfExecutor,
                    this.dfLib, this.mpiRunner, this.values, this.streams, this.prefixes, this.taskSandboxWorkingDir,
                    context.getThreadOutStream(), context.getThreadErrStream());
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
=======
            writer = new BufferedWriter(new FileWriter(filename));
            writer.write(workersInLines);
        } catch (IOException e) {
            throw new InvokeExecutionException("Error writing decaf hostfile", e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                // Nothing to do
            }
>>>>>>> Invokers don't use Generic invoker class
        }
        return filename;
    }

}
