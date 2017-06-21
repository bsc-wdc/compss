package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;

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
