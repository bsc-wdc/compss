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

    private final MethodImplementation impl;


    public JavaMethodDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + MethodImplementation.NUM_PARAMS);
        this.className = args[execArgsIdx++];
        this.methodName = args[execArgsIdx];

        this.impl = new MethodImplementation(this.className, this.methodName, null, null, null);
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
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
    public String toLogString() {
        return this.impl.getMethodDefinition();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Type: ").append(getType()).append("\n");
        sb.append("ClassName: ").append(this.className).append("\n");
        sb.append("MethodName: ").append(this.methodName).append("\n");
        sb.append(super.toString());
        return sb.toString();
    }

}
