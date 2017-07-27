package es.bsc.compss.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;


public class FakeActionOrchestrator implements ActionOrchestrator {

    private final TaskScheduler ts;


    public FakeActionOrchestrator(TaskScheduler ts) {
        super();
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        ts.actionCompleted(action);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
        ts.errorOnAction(action);
    }

}
