package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.resources.MethodResourceDescription;


public class FakeActionOrchestrator implements ActionOrchestrator<Profile, MethodResourceDescription, FakeImplementation> {

    private final TaskScheduler<Profile, MethodResourceDescription, FakeImplementation> ts;


    public FakeActionOrchestrator(TaskScheduler<Profile, MethodResourceDescription, FakeImplementation> ts) {
        super();
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> action) {
        ts.actionCompleted(action);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> action) {
        ts.errorOnAction(action);
    }

}