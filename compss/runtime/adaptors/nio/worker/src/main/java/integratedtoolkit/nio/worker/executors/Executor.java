package integratedtoolkit.nio.worker.executors;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.util.RequestQueue;
import integratedtoolkit.types.resources.MethodResourceDescription;


public abstract class Executor implements Runnable {

    protected static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    protected static final boolean workerDebug = logger.isDebugEnabled();

    // Attached component NIOWorker
    private final NIOWorker nw;
    // Attached component Jobs thread Pool
    protected final JobsThreadPool pool;
    // Attached component Request queue
    protected final RequestQueue<NIOTask> queue;


    public Executor(NIOWorker nw, JobsThreadPool pool, RequestQueue<NIOTask> queue) {
        logger.info("Executor init");
        this.nw = nw;
        this.pool = pool;
        this.queue = queue;
    }

    /**
     * Thread main code which enables the request processing
     */
    public void run() {
        // Main loop to process requests
        processRequests();

        // Close language specific properties
        finish();

        // Notify pool of thread end
        if (pool != null) {
            pool.threadEnd();
        }
    }

    private void processRequests() {
        NIOTask nt;
        while (true) {
            nt = queue.dequeue(); // Get tasks until there are no more tasks pending

            if (nt == null) {
                logger.debug("Dequeued job is null");
                break;
            }

            if (workerDebug) {
                logger.debug("Dequeuing job " + nt.getJobId());
            }

            boolean success = executeTask(nt);

            if (workerDebug) {
                logger.debug("Job " + nt.getJobId() + " finished (success: " + success + ")");
            }

            nw.sendTaskDone(nt, success);
        }
    }

    private boolean executeTask(NIOTask nt) {
        switch (Lang.valueOf(nt.getLang().toUpperCase())) {
            case JAVA:
            case PYTHON:
            case C:
                return execute(nt, nw);
            default:
                logger.error("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                // Print to the job.err file
                System.err.println("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                return false;
        }
    }

    public final boolean execute(NIOTask nt, NIOWorker nw) {
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.TASK_RUNNING.getId(), NIOTracer.Event.TASK_RUNNING.getType());
        }

        String workingDir = nw.getWorkingDir();

        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        String outputsBasename = workingDir + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist();

        // Sets the process environment variables (just in case its a MPI or OMPSs task)
        List<String> slaveWorkersHostnames = nt.getSlaveWorkersNodeNames();
        
        Set<String> hostnames = new HashSet<String>();
        hostnames.add(nw.getHostName());
        hostnames.addAll(nt.getSlaveWorkersNodeNames());
        boolean firstElement = true;
        StringBuilder hostnamesSTR = new StringBuilder();
        for (Iterator<String> it = hostnames.iterator(); it.hasNext(); ) {
            String hostname = it.next();
            if (firstElement) {
                firstElement = false;
                hostnamesSTR.append(hostname);
            } else {
                hostnamesSTR.append(",").append(hostname);
            }
        }

        int numNodes = slaveWorkersHostnames.size() + 1;
        int cus = nt.getResourceDescription().getTotalCPUComputingUnits();
        
        setEnvironmentVariables(hostnamesSTR.toString(), numNodes, cus, nt.getResourceDescription());

        // Execute task
        try {
            int[] assignedCoreUnits = nw.bindCoreUnits(nt.getJobId(), nt.getResourceDescription().getTotalCPUComputingUnits());
            int[] assignedGPUs = nw.bindGPUs(nt.getJobId(), nt.getResourceDescription().getTotalGPUComputingUnits());
            executeTask(nw, nt, outputsBasename, assignedCoreUnits, assignedGPUs);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        } finally {
            nw.releaseCoreUnits(nt.getJobId());
            nw.releaseGPUs(nt.getJobId());
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
            }
        }

        return true;
    }

    public abstract void setEnvironmentVariables(String hostnames, int numNodes, int cus, MethodResourceDescription reqs);

    public abstract void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename, int[] assignedCoreUnits, int[] assignedGPUs) throws Exception;

    public abstract void finish();

}
