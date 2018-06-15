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
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.invokers.util.GenericInvoker;


public class DecafInvoker extends Invoker {

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
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.mpiRunner = decafImpl.getMpiRunner();
        this.dfScript = decafImpl.getDfScript();
        this.dfExecutor = decafImpl.getDfExecutor();
        this.dfLib = decafImpl.getDfLib();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        checkArguments();
        return invokeDecafMethod();
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

    private Object invokeDecafMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.dfScript + " in " + this.context.getHostName());
        try {
            return GenericInvoker.invokeDecafMethod(context.getInstallDir() + DecafImplementation.SCRIPT_PATH, this.dfScript, this.dfExecutor,
                    this.dfLib, this.mpiRunner, this.values, this.streams, this.prefixes, this.taskSandboxWorkingDir,
                    context.getThreadOutStream(), context.getThreadErrStream());
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }
    }

}
