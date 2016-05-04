package integratedtoolkit.nio.worker.executors;

import java.io.File;

import integratedtoolkit.ITConstants;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.worker.NIOWorker;


public abstract class Executor {
	
    protected static final Logger logger = Logger.getLogger(Loggers.WORKER);
       
    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
    
    public final boolean execute(NIOTask nt, NIOWorker nw) {

        if (tracing){
            NIOTracer.emitEvent(NIOTracer.Event.TASK_RUNNING.getId() , NIOTracer.Event.TASK_RUNNING.getType());
        }
        
        NIOWorker.registerOutputs(NIOWorker.workingDir + File.separator + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist());
        String sandBox;
        try {
            logger.debug("Creating sandbox for job "+nt.getJobId());
            sandBox = createSandBox();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            NIOWorker.unregisterOutputs();
            if (tracing) {
            	NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
            }
            return false;
        }

        try {
            executeTask(sandBox, nt, nw);
        } catch (Exception e) {
        	logger.error(e.getMessage(), e);
            return false;
        } finally {
            try {
            	logger.debug("Removing sandbox for job " + nt.getJobId());
            	removeSandBox(sandBox);
            } catch (Exception e1) {
            	logger.error(e1.getMessage(), e1);
                return false;
            } finally {
                NIOWorker.unregisterOutputs();
                if (tracing) {
                    NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.TASK_RUNNING.getType());
                }
            }
        }

        return true;
    }

    abstract String createSandBox() throws Exception;

    abstract void executeTask(String sandBox, NIOTask nt, NIOWorker nw) throws Exception;

    abstract void removeSandBox(String sandBox) throws Exception;
}
