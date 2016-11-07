package integratedtoolkit.types.allocatiableaction;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.allocatiableaction.AllocatableActionTest.ResourceDependencies;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.LinkedList;


public class DummyResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ResourceScheduler<P, T> {

    public DummyResourceScheduler(Worker<T> w) {
        super(w);
    }

    @Override
    public LinkedList<AllocatableAction<P, T>> unscheduleAction(AllocatableAction<P, T> action) {

        LinkedList<AllocatableAction<P, T>> freeTasks = new LinkedList<>();
        ResourceDependencies<P, T> actionDSI = (ResourceDependencies<P, T>) action.getSchedulingInfo();

        // Remove action from predecessors
        for (AllocatableAction<P, T> pred : actionDSI.getPredecessors()) {
            ResourceDependencies<P, T> predDSI = (ResourceDependencies<P, T>) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        for (AllocatableAction<P, T> successor : actionDSI.getSuccessors()) {
            ResourceDependencies<P, T> successorDSI = (ResourceDependencies<P, T>) successor.getSchedulingInfo();
            // Remove predecessor
            successorDSI.removePredecessor(action);

            // Link with action predecessors
            for (AllocatableAction<P, T> predecessor : actionDSI.getPredecessors()) {
                ResourceDependencies<P, T> predecessorDSI = (ResourceDependencies<P, T>) predecessor.getSchedulingInfo();
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
