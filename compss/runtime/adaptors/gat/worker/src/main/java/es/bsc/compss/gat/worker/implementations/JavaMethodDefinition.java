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

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MethodImplementation;


public class JavaMethodDefinition extends ImplementationDefinition {

    private final String className;
    private final String methodName;

    public JavaMethodDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + 2);
        System.out.println("Class Name = args[" + execArgsIdx + "]=" + args[execArgsIdx]);
        this.className = args[execArgsIdx++];
        System.out.println("Method Name = args[" + execArgsIdx + "]=" + args[execArgsIdx]);
        this.methodName = args[execArgsIdx];
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
    public Lang getLang() {
        return Lang.JAVA;
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: ").append(getType()).append("\n");
        sb.append("ClassName: ").append(className).append("\n");
        sb.append("MethodName: ").append(methodName).append("\n");
        sb.append(super.toString());
        return sb.toString();
    }

}
