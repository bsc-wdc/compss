/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.MethodType;


public class MPIDefinition extends ImplementationDefinition {

    private final String mpiRunner;
    private final String mpiBinary;
    private final String workingDir;

    private final MPIImplementation impl;


    /**
     * Creates a new MPI definition implementation.
     * 
     * @param debug Whether the debug mode is enabled or not.
     * @param args Application arguments.
     * @param execArgsIdx Index of the start of the execution arguments.
     */
    public MPIDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + MPIImplementation.NUM_PARAMS);

        this.mpiRunner = args[execArgsIdx++];
        this.mpiBinary = args[execArgsIdx++];

        String wDir = args[execArgsIdx++];
        if (wDir == null || wDir.isEmpty() || wDir.equals(Constants.UNASSIGNED)) {
            this.workingDir = null;
        } else {
            this.workingDir = wDir;
        }

        this.impl = new MPIImplementation(this.mpiBinary, this.workingDir, this.mpiRunner, null, null, "", null);
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public MethodType getType() {
        return MethodType.MPI;
    }

    @Override
    public String toLogString() {
        return this.impl.getMethodDefinition();
    }

}
