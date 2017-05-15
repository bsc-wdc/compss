package integratedtoolkit.nio.worker.executors;

import java.io.File;

import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.exceptions.JobExecutionException;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.executors.util.BinaryInvoker;
import integratedtoolkit.nio.worker.executors.util.Invoker;
import integratedtoolkit.nio.worker.executors.util.JavaInvoker;
import integratedtoolkit.nio.worker.executors.util.MPIInvoker;
import integratedtoolkit.nio.worker.executors.util.OmpSsInvoker;
import integratedtoolkit.nio.worker.executors.util.OpenCLInvoker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.implementations.AbstractMethodImplementation.MethodType;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.RequestQueue;


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
            int[] assignedGPUs) throws JobExecutionException {
        /* Register outputs **************************************** */
        NIOWorker.registerOutputs(outputsBasename);

        /* TRY TO PROCESS THE TASK ******************************** */
        System.out.println("[JAVA EXECUTOR] executeTask - Begin task execution");
        try {
            MethodType methodType = nt.getMethodType();
            Invoker invoker = null;
            switch (methodType) {
                case METHOD:
                    invoker = new JavaInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case MPI:
                    invoker = new MPIInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case OMPSS:
                    invoker = new OmpSsInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case OPENCL:
                    invoker = new OpenCLInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
                    break;
                case BINARY:
                    invoker = new BinaryInvoker(nw, nt, taskSandboxWorkingDir, assignedCoreUnits);
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

}
