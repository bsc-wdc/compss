package es.bsc.compss.util;

import es.bsc.compss.components.impl.TaskScheduler;

public class SchedulingOptimizer<T extends TaskScheduler> extends Thread {

    private static final String SCHEDULING_OPTIMIZER_THREAD_NAME = "Task Optimizer";
    protected T scheduler;

    public SchedulingOptimizer(T ts) {
        this.setName(SCHEDULING_OPTIMIZER_THREAD_NAME);
        this.scheduler = ts;
    }

    @Override
    public void run() {

    }

    public void shutdown() {
    }

}
