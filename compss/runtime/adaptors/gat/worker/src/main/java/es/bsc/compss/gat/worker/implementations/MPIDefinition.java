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
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MPIImplementation;


public class MPIDefinition extends ImplementationDefinition {

    private final String mpiRunner;
    private final String mpiBinary;
    private final String workingDir;

    public MPIDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + 3);

        this.mpiRunner = args[execArgsIdx++];
        this.mpiBinary = args[execArgsIdx++];
        String wDir = args[execArgsIdx++];
        if ((wDir == null || wDir.isEmpty() || wDir.equals(Constants.UNASSIGNED))) {
            this.workingDir = null;
        } else {
            this.workingDir = wDir;
        }
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return new MPIImplementation(mpiBinary, this.workingDir, mpiRunner, null, null, null);
    }

    @Override
    public MethodType getType() {
        return MethodType.MPI;
    }

    @Override
    public Lang getLang() {
        return null;
    }

    @Override
    public String toCommandString() {
        return mpiRunner + " " + mpiBinary;
    }

    @Override
    public String toLogString() {
        return "["
                + "MPI RUNNER=" + mpiRunner
                + ", BINARY=" + mpiBinary
                + "]";
    }

}
