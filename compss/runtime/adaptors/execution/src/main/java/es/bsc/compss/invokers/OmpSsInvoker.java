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
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Stream;
import java.io.PrintStream;
import java.util.ArrayList;


public class OmpSsInvoker extends Invoker {

    private static final int NUM_BASE_OMPSS_ARGS = 1;

    private final String ompssBinary;

    public OmpSsInvoker(InvocationContext context, Invocation invocation, boolean debug, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(context, invocation, debug, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        OmpSsImplementation ompssImpl = null;
        try {
            ompssImpl = (OmpSsImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.ompssBinary = ompssImpl.getBinary();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + ompssBinary + " in " + context.getHostName());
        try {
            return runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {

        System.out.println("");
        System.out.println("[OMPSS INVOKER] Begin ompss call to " + this.ompssBinary);
        System.out.println("[OMPSS INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());

        // Get COMPSS ENV VARS
        String computingUnits = System.getProperty(Constants.COMPSS_NUM_THREADS);
        System.out.println("[OMPSS INVOKER] COMPSS_NUM_THREADS: " + computingUnits);

        // Command similar to
        // ./exec args
        // Convert binary parameters and calculate binary-streams redirection
        BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.values, this.streams, this.prefixes, streamValues);

        // Prepare command
        String[] cmd = new String[NUM_BASE_OMPSS_ARGS + binaryParams.size()];
        cmd[0] = this.ompssBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_OMPSS_ARGS + i] = binaryParams.get(i);
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, computingUnits);

        // Debug command
        System.out.print("[OMPSS INVOKER] BINARY CMD: ");
        for (int i = 0; i < cmd.length; ++i) {
            System.out.print(cmd[i] + " ");
        }
        System.out.println("");
        System.out.println("[OMPSS INVOKER] OmpSs STDIN: " + streamValues.getStdIn());
        System.out.println("[OMPSS INVOKER] OmpSs STDOUT: " + streamValues.getStdOut());
        System.out.println("[OMPSS INVOKER] OmpSs STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(), context.getThreadErrStream());
    }
}
