package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.util.LinkedList;
import org.json.JSONObject;

public class FakeResourceScheduler extends ResourceScheduler<MethodResourceDescription> {

    public FakeResourceScheduler(FakeWorker w, JSONObject json) {
        super(w, json);
    }

    @Override
    public LinkedList<AllocatableAction> unscheduleAction(AllocatableAction action) {

        LinkedList<AllocatableAction> freeTasks = new LinkedList<>();
        FakeSI actionDSI = (FakeSI) action.getSchedulingInfo();

        // Remove action from predecessors
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            FakeSI predDSI = (FakeSI) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            FakeSI successorDSI = (FakeSI) successor.getSchedulingInfo();
            // Remove predecessor
            successorDSI.removePredecessor(action);

            // Link with action predecessors
            for (AllocatableAction predecessor : actionDSI.getPredecessors()) {
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
