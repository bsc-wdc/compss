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
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import java.io.PrintStream;
import java.util.ArrayList;


public class OmpSsInvoker extends Invoker {

    private static final int NUM_BASE_OMPSS_ARGS = 1;

    private final String ompssBinary;
    private final int computingUnits;

    public OmpSsInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        OmpSsImplementation ompssImpl = null;
        try {
            ompssImpl = (OmpSsImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }
        this.ompssBinary = ompssImpl.getBinary();
        ResourceDescription rd = invocation.getRequirements();
        if (invocation.getTaskType() == Implementation.TaskType.METHOD) {
            this.computingUnits = ((MethodResourceDescription) rd).getTotalCPUComputingUnits();
        } else {
            this.computingUnits = 0;
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + ompssBinary + " in " + context.getHostName());
        try {
            Object retValue = runInvocation();
            for (InvocationParam np : this.invocation.getResults()) {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
            }
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        // Command similar to
        // ./exec args
        // Convert binary parameters and calculate binary-streams redirection
        BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(invocation.getParams(), invocation.getTarget(), streamValues);

        // Prepare command
        String[] cmd = new String[NUM_BASE_OMPSS_ARGS + binaryParams.size()];
        cmd[0] = this.ompssBinary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_OMPSS_ARGS + i] = binaryParams.get(i);
        }

        // Prepare environment
        System.setProperty(OMP_NUM_THREADS, String.valueOf(computingUnits));

        if (invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[OMPSS INVOKER] Begin ompss call to " + this.ompssBinary);
            outLog.println("[OMPSS INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());

            outLog.println("[OMPSS INVOKER] COMPSS_NUM_THREADS: " + computingUnits);
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
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(), context.getThreadErrStream());
    }
}
