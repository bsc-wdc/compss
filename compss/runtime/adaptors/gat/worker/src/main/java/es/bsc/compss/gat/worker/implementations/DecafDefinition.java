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
import es.bsc.compss.invokers.DecafInvoker;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.util.ErrorManager;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;


public class DecafDefinition extends ImplementationDefinition {

    private final String dfScript;
    private final String dfExecutor;
    private final String dfLib;
    private final String mpiRunner;

    private final int numNodes;
    private final String hostnames;
    private final int cus;

    public DecafDefinition(String[] args, int execArgsIdx) {
        super(args, execArgsIdx + 4);
        this.dfScript = args[execArgsIdx++];
        this.dfExecutor = args[execArgsIdx++];
        this.dfLib = args[execArgsIdx++];
        this.mpiRunner = args[execArgsIdx++];

        int numNodesTmp = Integer.parseInt(args[execArgsIdx++]);
        ArrayList<String> hostnamesList = new ArrayList<>();
        for (int i = 0; i < numNodesTmp; ++i) {
            String nodeName = args[execArgsIdx++];
            if (nodeName.endsWith("-ib0")) {
                nodeName = nodeName.substring(0, nodeName.lastIndexOf("-ib0"));
            }
            hostnamesList.add(nodeName);
        }
        String hostname = "localhost";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e1) {
            ErrorManager.warn("Cannot obtain hostname. Loading default value " + hostname);
        }
        hostnamesList.add(hostname);
        numNodesTmp++;
        this.numNodes = numNodesTmp;
        cus = Integer.parseInt(args[execArgsIdx++]);
        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (String nodeName : hostnamesList) {
            // Add one host name per process to launch
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(nodeName);
                for (int i = 1; i < cus; ++i) {
                    hostnamesSTR.append(",").append(nodeName);
                }
            } else {
                for (int i = 0; i < cus; ++i) {
                    hostnamesSTR.append(",").append(nodeName);
                }
            }
        }
        this.hostnames = hostnamesSTR.toString();
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

    @Override
    public Invoker getInvoker(InvocationContext context, boolean debug, File sandBoxDir) throws JobExecutionException {
        return new ExtendedInvoker(context, this, debug, sandBoxDir, null);
    }


    private class ExtendedInvoker extends DecafInvoker {

        final boolean debug;

        public ExtendedInvoker(InvocationContext context, Invocation invocation, boolean debug, File taskSandboxWorkingDir, int[] assignedCoreUnits) throws JobExecutionException {
            super(context, invocation, debug, taskSandboxWorkingDir, assignedCoreUnits);
            this.debug = debug;
        }

        @Override
        public Object invokeMethod() throws JobExecutionException {
            setEnvironmentVariables();
            return super.invokeMethod();
        }

        private void setEnvironmentVariables() {
            if (debug) {
                System.out.println("  * HOSTNAMES: " + DecafDefinition.this.hostnames);
                System.out.println("  * NUM_NODES: " + DecafDefinition.this.numNodes);
                System.out.println("  * CPU_COMPUTING_UNITS: " + DecafDefinition.this.cus);
            }
            System.setProperty(Constants.COMPSS_HOSTNAMES, DecafDefinition.this.hostnames);
            System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(DecafDefinition.this.numNodes));
            System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(DecafDefinition.this.cus));
        }
    }
}
