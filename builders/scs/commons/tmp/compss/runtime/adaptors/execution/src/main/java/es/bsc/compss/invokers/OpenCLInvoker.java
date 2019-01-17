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

import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.OpenCLImplementation;

import java.io.File;


public class OpenCLInvoker extends Invoker {

    private final String kernel;


    public OpenCLInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        OpenCLImplementation openclImpl = null;
        try {
            openclImpl = (OpenCLImplementation) this.invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }
        this.kernel = openclImpl.getKernel();
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        // TODO: Handle OpenCL invoke
        throw new JobExecutionException("Unsupported Method Type OPENCL with kernel" + this.kernel);
    }

}
