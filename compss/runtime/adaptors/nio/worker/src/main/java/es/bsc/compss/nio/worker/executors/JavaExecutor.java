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
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.COMPSsConstants;
import java.io.File;

import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.exceptions.JobExecutionException;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.invokers.BinaryInvoker;
import es.bsc.compss.invokers.DecafInvoker;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.invokers.MPIInvoker;
import es.bsc.compss.invokers.OmpSsInvoker;
import es.bsc.compss.invokers.OpenCLInvoker;
import es.bsc.compss.invokers.StorageInvoker;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.RequestQueue;


public class JavaExecutor extends Executor {

    public JavaExecutor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        super(nw, pool, queue);
    }

    @Override
    public void setEnvironmentVariables(String hostnames, int numNodes, int cus, MethodResourceDescription reqs) {
        if (LOGGER.isDebugEnabled()) {
            System.out.println("HOSTNAMES: " + hostnames);
            System.out.println("NUM_NODES: " + numNodes);
            System.out.println("CPU_COMPUTING_UNITS: " + cus);
        }

        System.setProperty(Constants.COMPSS_HOSTNAMES, hostnames.toString());
        System.setProperty(Constants.COMPSS_NUM_NODES, String.valueOf(numNodes));
        System.setProperty(Constants.COMPSS_NUM_THREADS, String.valueOf(cus));
    }

    @Override
    public void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, File taskSandboxWorkingDir, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) throws JobExecutionException {
        /* Register outputs **************************************** */
        NIOWorker.registerOutputs(outputsBasename);

        /* TRY TO PROCESS THE TASK ******************************** */
        System.out.println("[JAVA EXECUTOR] executeTask - Begin task execution");
        try {
            MethodType methodType = nt.getMethodImplementation().getMethodType();
            Invoker invoker = null;
            switch (methodType) {
                case METHOD:
                    if (NIOWorker.getExecutionType().equals(COMPSsConstants.EXECUTION_INTERNAL)) {
                        invoker = new JavaInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    } else {
                        invoker = new StorageInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    }
                    break;
                case MPI:
                    invoker = new MPIInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case DECAF:
                    invoker = new DecafInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case OMPSS:
                    invoker = new OmpSsInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case OPENCL:
                    invoker = new OpenCLInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case BINARY:
                    invoker = new BinaryInvoker(nw, nt, nw.isWorkerDebugEnabled(), taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                default:
                    throw new JobExecutionException("Unrecognised method type");
            }
            invoker.processTask();
        } catch (JobExecutionException jee) {
            System.out.println("[JAVA EXECUTOR] executeTask - Error in task execution");
            System.err.println("[JAVA EXECUTOR] executeTask - Error in task execution");
            jee.printStackTrace();
            throw jee;
        } finally {
            System.out.println("[JAVA EXECUTOR] executeTask - End task execution");
            NIOWorker.unregisterOutputs();
        }
    }

    @Override
    public void finish() {
        // Nothing to do since everything is deleted in each task execution
        LOGGER.info("Executor finished");
    }

    @Override
    public void start() {
        // Nothing to do since everything is deleted in each task execution
        LOGGER.info("Executor started");
    }

}
