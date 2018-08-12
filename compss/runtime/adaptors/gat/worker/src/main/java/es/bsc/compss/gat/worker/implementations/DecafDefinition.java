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
import es.bsc.compss.types.implementations.DecafImplementation;


public class DecafDefinition extends ImplementationDefinition {

    private final String dfScript;
    private final String dfExecutor;
    private final String dfLib;
    private final String mpiRunner;

    public DecafDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + 4);
        this.dfScript = args[execArgsIdx++];
        this.dfExecutor = args[execArgsIdx++];
        this.dfLib = args[execArgsIdx++];
        this.mpiRunner = args[execArgsIdx++];
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return new DecafImplementation(dfScript, dfExecutor, dfLib, "", mpiRunner, null, null, null);
    }

    @Override
    public MethodType getType() {
        return MethodType.DECAF;
    }

    @Override
    public Lang getLang() {
        return null;
    }

    @Override
    public String toCommandString() {
        return dfScript + " " + dfExecutor + " " + dfLib + " " + mpiRunner;
    }

    @Override
    public String toLogString() {
        return "["
                + "DF SCRIPT=" + dfScript
                + "DF EXECUTOR=" + dfExecutor
                + "DF LIB=" + dfLib
                + "MPI RUNNER=" + mpiRunner
                + "]";
    }

}
