package es.bsc.compss.nio.listeners;

import es.bsc.compss.executor.types.ExecutionListener;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.worker.COMPSsException;


public class TaskExecutionListener implements ExecutionListener {

    private final NIOWorker nw;


    /**
     * Creates a listener for a task execution.
     * 
     * @param nw Associated NIOWorker.
     */
    public TaskExecutionListener(NIOWorker nw) {
        this.nw = nw;
    }

    @Override
    public void notifyEnd(Invocation invocation, boolean success, COMPSsException exception) {
        this.nw.sendTaskDone(invocation, success, exception);
    }

}
