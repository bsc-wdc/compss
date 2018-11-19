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
import es.bsc.compss.types.implementations.COMPSsImplementation;
import java.util.ArrayList;
import java.util.UUID;


public class COMPSsInvoker extends Invoker {

    private static final int NUM_BASE_COMPSS_ARGS = 6;

    private static final String ERROR_RUNCOMPSS = "ERROR: Invalid runcompss";
    private static final String ERROR_APP_NAME = "ERROR: Invalid appName";
    private static final String ERROR_TARGET_PARAM = "ERROR: COMPSs Execution doesn't support target parameters";

    private final String runcompss;
    private final String extraFlags;
    private final String appName;


    public COMPSsInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);
        // Get method definition properties
        COMPSsImplementation compssImpl = null;
        try {
            compssImpl = (COMPSsImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // COMPSs specific flags
        this.runcompss = compssImpl.getRuncompss();
        this.extraFlags = compssImpl.getFlags();
        this.appName = compssImpl.getAppName();
    }

    private void checkArguments() throws JobExecutionException {
        if (this.runcompss == null || this.runcompss.isEmpty()) {
            throw new JobExecutionException(ERROR_RUNCOMPSS);
        }
        if (this.appName == null || this.appName.isEmpty()) {
            throw new JobExecutionException(ERROR_APP_NAME);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.appName + " in " + this.context.getHostName());
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Check results
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
        System.out.println("");
        System.out.println("[COMPSs INVOKER] Begin COMPSs call to " + appName);
        System.out.println("[COMPSs INVOKER] On WorkingDir : " + taskSandboxWorkingDir.getAbsolutePath());

        // Command similar to
        // export OMP_NUM_THREADS=1 ; runcompss --project=tmp_proj.xml --resources=tmp_res.xml [-extra_flags] appName
        // appArgs

        // Generate XML files
        String uuid = UUID.randomUUID().toString();
        String project_xml = "project_" + uuid + ".xml";
        String resources_xml = "resources_" + uuid + ".xml";
        // TODO: Generate XML files

        // Convert binary parameters and calculate binary-streams redirection
        StreamSTD streamValues = new StreamSTD();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(invocation.getParams(), invocation.getTarget(),
                streamValues);

        // Prepare command
        String[] extraFlags_array = (extraFlags != null && !extraFlags.isEmpty()) ? extraFlags.split(" ") : new String[0];

        String[] cmd = new String[NUM_BASE_COMPSS_ARGS + extraFlags_array.length + binaryParams.size()];
        cmd[0] = runcompss;
        cmd[1] = "--project=" + project_xml;
        cmd[2] = "--resources=" + resources_xml;
        int i = NUM_BASE_COMPSS_ARGS;
        for (String flag : extraFlags_array) {
            cmd[i++] = flag;
        }
        cmd[i++] = appName;
        for (String param : binaryParams) {
            cmd[i++] = param;
        }

        // Debug command
        System.out.print("[COMPSs INVOKER] COMPSs CMD: ");
        for (int index = 0; index < cmd.length; ++index) {
            System.out.print(cmd[index] + " ");
        }
        System.out.println("");
        System.out.println("[COMPSs INVOKER] COMPSs STDIN: " + streamValues.getStdIn());
        System.out.println("[COMPSs INVOKER] COMPSs STDOUT: " + streamValues.getStdOut());
        System.out.println("[COMPSs INVOKER] COMPSs STDERR: " + streamValues.getStdErr());

        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, context.getThreadOutStream(),
                context.getThreadErrStream());
    }
}