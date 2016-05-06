package integratedtoolkit.nio.worker;

import org.apache.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.executors.CExecutor;
import integratedtoolkit.nio.worker.executors.JavaExecutor;
import integratedtoolkit.nio.worker.executors.PythonExecutor;
import integratedtoolkit.util.RequestDispatcher;
import integratedtoolkit.util.RequestQueue;


public class JobLauncher extends RequestDispatcher<NIOTask> {

    protected static final int NUM_HEADER_PARS = 5;

    private final NIOWorker nw;
    private final JavaExecutor java = new JavaExecutor();
    private final CExecutor c = new CExecutor();
    private final PythonExecutor python = new PythonExecutor();

	private Logger logger;

	private boolean workerDebug = false;

    public JobLauncher(RequestQueue<NIOTask> queue, NIOWorker nw, Logger logger, boolean workerDebug) {
        super(queue);
        this.nw = nw;
        this.logger = logger;
        this.workerDebug = workerDebug;
    }

    public void processRequests() {
        NIOTask nt;

        while (true) {
        	
            nt = queue.dequeue();   // Get tasks until there are no more tasks pending
            
            if (nt == null) {
            	logger.debug("Dequeued job is null"); 
            	break;
            }
            
            if (workerDebug){
            	logger.debug("Dequeuing job "+ nt.getJobId());
            }
            
            boolean success = executeTask(nt);
            
            if (workerDebug){
            	logger.debug("Job "+ nt.getJobId()+" finished (success: "+ success + ")");
            }
            
            nw.sendTaskDone(nt, success);
        }

    }

    private boolean executeTask(NIOTask nt) {
        switch (Lang.valueOf(nt.getLang().toUpperCase())) {
            case JAVA:
                return java.execute(nt, nw);
            case PYTHON:
                return python.execute(nt, nw);
            case C:
                return c.execute(nt, nw);
            default:
            	// Print to the job.err file
                System.err.println("Incorrect language " + nt.getLang() + " in job " + nt.getJobId());
                return false;
        }
    }

}
