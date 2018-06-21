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

import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MethodImplementation;
import java.io.File;


public class MethodDefinition extends ImplementationDefinition {

    private final String className;
    private final String methodName;

    public MethodDefinition(String[] args, int execArgsIdx) {
        super(args, execArgsIdx + 2);
        this.className = args[execArgsIdx];
        this.methodName = args[execArgsIdx + 1];
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return new MethodImplementation(className, methodName, null, null, null);
    }

    @Override
    public MethodType getType() {
        return MethodType.METHOD;
    }

    @Override
    public String toCommandString() {
        return className + " " + methodName;
    }

    @Override
    public String toLogString() {
        return "["
                + "DECLARING CLASS=" + className
                + ", METHOD NAME=" + methodName
                + "]";
    }

    @Override
    public Invoker getInvoker(InvocationContext context, boolean debug, File sandBoxDir) throws JobExecutionException {
        return new JavaInvoker(context, this, debug, sandBoxDir, null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: ").append(getType()).append("\n");
        sb.append("ClassName: ").append(className).append("\n");
        sb.append("MethodName: ").append(methodName).append("\n");
        sb.append(super.toString());
        return sb.toString();
    }
}
