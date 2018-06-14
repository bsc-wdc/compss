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
package es.bsc.compss.nio.worker.executors.util;

import java.io.File;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.execution.Invocation;

import es.bsc.compss.types.implementations.BinaryImplementation;

import es.bsc.compss.worker.invokers.GenericInvoker;


public class BinaryInvoker extends Invoker {

    private final String binary;


    public BinaryInvoker(NIOWorker nw, Invocation nt, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
        super(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);

        // Get method definition properties
        BinaryImplementation binaryImpl = null;
        try {
            binaryImpl = (BinaryImplementation) this.impl;
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + this.methodType, e);
        }
        this.binary = binaryImpl.getBinary();
    }

    @Override
    public Object invokeMethod() throws JobExecutionException {
        LOGGER.info("Invoked " + this.binary + " in " + this.nw.getHostName());
        try {
            
            return GenericInvoker.invokeBinaryMethod(this.binary, this.values, this.streams, this.prefixes, 
                    this.taskSandboxWorkingDir, nw.getThreadOutStream(), nw.getThreadErrStream());
        } catch (InvokeExecutionException iee) {
            LOGGER.error("Exception running binary", iee);
            throw new JobExecutionException(iee);
        }
    }

}
