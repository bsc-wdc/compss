package integratedtoolkit.nio.worker.executors;

import java.io.File;

import integratedtoolkit.ITConstants;

import org.apache.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.util.RequestQueue;


public abstract class Executor implements Runnable {
	
    protected static final Logger logger = Logger.getLogger(Loggers.WORKER_EXECUTOR);
    protected static final boolean workerDebug = logger.isDebugEnabled();
       
    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
   
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
			nt = queue.dequeue(); 	// Get tasks until there are no more tasks pending

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
        if (tracing){
            NIOTracer.emitEvent(NIOTracer.Event.TASK_RUNNING.getId() , NIOTracer.Event.TASK_RUNNING.getType());
        }
        
        String workingDir = nw.getWorkingDir();
        
        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        String outputsBasename = workingDir + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist();

        // Execute task
        try {
            executeTask(nw, nt, outputsBasename);
        } catch (Exception e) {
        	logger.error(e.getMessage(), e);
            return false;
        } finally {
            if (tracing) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
            }
        }

        return true;
    }

    public abstract void executeTask(NIOWorker nw, NIOTask nt, String outputsBasename) throws Exception;
    
    public abstract void finish();
    
}
