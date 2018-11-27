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
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.BinaryImplementation;
import java.io.PrintStream;

import java.util.ArrayList;


public class BinaryInvoker extends Invoker {

    private static final int NUM_BASE_BINARY_ARGS = 1;

    private final String binary;


    public BinaryInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        BinaryImplementation binaryImpl = null;
        try {
            binaryImpl = (BinaryImplementation) invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }
        this.binary = binaryImpl.getBinary();
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.binary + " in " + this.context.getHostName());
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            LOGGER.error("Exception running binary", iee);
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
        // ./exec args
        // Convert binary parameters and calculate binary-streams redirection
        BinaryRunner.StreamSTD streamValues = new BinaryRunner.StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(invocation.getParams(), invocation.getTarget(),
                streamValues);

        // Prepare command
        String[] cmd = new String[NUM_BASE_BINARY_ARGS + binaryParams.size()];
        cmd[0] = this.binary;
        for (int i = 0; i < binaryParams.size(); ++i) {
            cmd[NUM_BASE_BINARY_ARGS + i] = binaryParams.get(i);
        }

        if (invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[BINARY INVOKER] Begin binary call to " + this.binary);
            outLog.println("[BINARY INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());
            // Debug command
            outLog.print("[BINARY INVOKER] BINARY CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[BINARY INVOKER] Binary STDIN: " + streamValues.getStdIn());
            outLog.println("[BINARY INVOKER] Binary STDOUT: " + streamValues.getStdOut());
            outLog.println("[BINARY INVOKER] Binary STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(),
                context.getThreadErrStream());
    }

}
