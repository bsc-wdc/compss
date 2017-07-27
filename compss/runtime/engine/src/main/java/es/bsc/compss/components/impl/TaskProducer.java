package es.bsc.compss.components.impl;

import es.bsc.compss.types.Task;


/**
 * Abstract interface for task producer implementations
 *
 */
public interface TaskProducer {

    /**
     * Notifies the end of task @task
     * 
     * @param task
     */
    public void notifyTaskEnd(Task task);

}
