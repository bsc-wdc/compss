package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;


public class FakeActionOrchestrator implements ActionOrchestrator<FakeProfile, FakeResourceDescription, FakeImplementation> {

    private final TaskScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ts;


    public FakeActionOrchestrator(TaskScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> ts) {
        super();
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> action) {
        ts.actionCompleted(action);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> action) {
        ts.errorOnAction(action);
    }

}
