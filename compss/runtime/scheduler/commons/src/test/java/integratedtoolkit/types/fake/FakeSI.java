package integratedtoolkit.types.fake;

import java.util.LinkedList;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.types.resources.MethodResourceDescription;


public class FakeSI extends SchedulingInformation<Profile, MethodResourceDescription, FakeImplementation> {

    // Allocatable actions that the action depends on due to resource availability
    private final LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> resourcePredecessors;

    // Allocatable actions depending on the allocatable action due to resource availability
    private final LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> resourceSuccessors;


    public FakeSI() {
        resourcePredecessors = new LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>>();
        resourceSuccessors = new LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>>();
    }

    public void addPredecessor(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        return resourcePredecessors.isEmpty();
    }

    public LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> getPredecessors() {
        return resourcePredecessors;
    }

    public void removePredecessor(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> successor) {
        resourcePredecessors.remove(successor);
    }

    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> successor) {
        resourceSuccessors.add(successor);
    }

    public LinkedList<AllocatableAction<Profile, MethodResourceDescription, FakeImplementation>> getSuccessors() {
        return resourceSuccessors;
    }

    public synchronized void removeSuccessor(AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> successor) {
        resourceSuccessors.remove(successor);
    }

    public void clearSuccessors() {
        resourceSuccessors.clear();
    }

}