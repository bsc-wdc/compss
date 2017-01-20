package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.util.LinkedList;


public class FakeResourceScheduler extends ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation> {

    public FakeResourceScheduler(FakeWorker w) {
        super(w);
    }

    @Override
    public LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> unscheduleAction(
            AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> action) {

        LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> freeTasks = new LinkedList<>();
        FakeSI actionDSI = (FakeSI) action.getSchedulingInfo();

        // Remove action from predecessors
        for (AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> pred : actionDSI.getPredecessors()) {
            FakeSI predDSI = (FakeSI) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        for (AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> successor : actionDSI.getSuccessors()) {
            FakeSI successorDSI = (FakeSI) successor.getSchedulingInfo();
            // Remove predecessor
            successorDSI.removePredecessor(action);

            // Link with action predecessors
            for (AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> predecessor : actionDSI.getPredecessors()) {
                FakeSI predecessorDSI = (FakeSI) predecessor.getSchedulingInfo();
                if (predecessor.isPending()) {
                    successorDSI.addPredecessor(predecessor);
                    predecessorDSI.addSuccessor(successor);
                }
            }
            // Check executability
            if (successorDSI.isExecutable()) {
                freeTasks.add(successor);
            }
        }
        actionDSI.clearPredecessors();
        actionDSI.clearSuccessors();

        return freeTasks;
    }

}
