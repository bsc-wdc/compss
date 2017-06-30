package integratedtoolkit.util;

import integratedtoolkit.components.impl.TaskScheduler;

public class SchedulingOptimizer<T extends TaskScheduler> extends Thread {

    private static final String SCHEDULING_OPTIMIZER_THREAD_NAME = "Task Optimizer";
    protected T scheduler;

    public SchedulingOptimizer(T TaskScheduler) {
        this.setName(SCHEDULING_OPTIMIZER_THREAD_NAME);
    }

    @Override
    public void run() {

    }

    public void shutdown() {
    }

}
