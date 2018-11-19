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
import es.bsc.compss.types.implementations.OpenCLImplementation;


public class OpenCLDefinition extends ImplementationDefinition {

    private final String kernel;

    private final OpenCLImplementation impl;


    public OpenCLDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + 1);
        this.kernel = args[execArgsIdx];

        this.impl = new OpenCLImplementation(this.kernel, "", null, null, null);
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public MethodType getType() {
        return MethodType.OPENCL;
    }

    @Override
    public Lang getLang() {
        return null;
    }

    @Override
    public String toLogString() {
        return this.impl.getMethodDefinition();
    }

}
