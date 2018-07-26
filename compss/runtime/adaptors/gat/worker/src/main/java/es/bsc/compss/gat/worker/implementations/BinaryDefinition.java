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
package es.bsc.compss.gat.worker.implementations;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.invokers.binary.BinaryInvoker;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.BinaryImplementation;
import java.io.File;


public class BinaryDefinition extends ImplementationDefinition {

    private final String binary;

    public BinaryDefinition(boolean enableDebug, String[] args, int execArgsIdx) {
        super(enableDebug, args, execArgsIdx + 1);
        this.binary = args[execArgsIdx];
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return new BinaryImplementation(binary, "", null, null, null);
    }

    @Override
    public MethodType getType() {
        return MethodType.BINARY;
    }

    @Override
    public COMPSsConstants.Lang getLang() {
        return null;
    }

    @Override
    public String toCommandString() {
        return binary;
    }

    @Override
    public String toLogString() {
        return "["
                + "BINARY=" + binary
                + "]";
    }

    @Override
    public Invoker getInvoker(InvocationContext context, File sandBoxDir) throws JobExecutionException {
        return new BinaryInvoker(context, this, sandBoxDir, null);
    }
}
