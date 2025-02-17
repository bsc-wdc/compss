/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.Validator;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public class FakeAllocatableAction extends AllocatableAction {

    private Validator validator;

    private int fakeId;
    private final TaskDescription taskDescription;


    /**
     * Creates a new FakeAllocatableAction instance.
     * 
     * @param td Associated task dispatcher.
     * @param id AllocatableAction id.
     * @param ce Core Element executed by the Action
     * @param v Validator that controls the overall execution
     */
    public FakeAllocatableAction(ActionOrchestrator td, int id, CoreElement ce, Validator v) {
        super(new FakeSI(null), td);
        this.fakeId = id;
        this.taskDescription = new TaskDescription(null, null, ce.getSignature(), ce, null, false, 1, false, false,
            false, false, 0, null, 0, new LinkedList<>());
        this.validator = v;
    }

    public int getFakeId() {
        return this.fakeId;
    }

    @Override
    public void doAction() {
        this.validator.executingAction(this);
    }

    @Override
    protected void doAbort() {
        // Nothing to do
    }

    @Override
    public void doCompleted() {
        this.validator.finishedAction(this);
    }

    @Override
    public void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    public void doFailed() {
        // Nothing to do
    }

    @Override
    protected boolean doCanceled() {
        // Nothing to do
        return true;
    }

    @Override
    protected void doFailIgnored() {
        // Nothing to do
    }

    @Override
    protected Collection<AllocatableAction> doException(COMPSsException e) {
        return new LinkedList<>();
    }

    @Override
    public String toString() {
        return "AllocatableAction " + this.fakeId;
    }

    @Override
    public <T extends WorkerResourceDescription> List<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r) {

        return r.getExecutableImpls(this.taskDescription.getCoreElement().getCoreId());
    }

    @Override
    public List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return getCoreElementExecutors(this.taskDescription.getCoreElement().getCoreId());
    }

    @Override
    public Implementation[] getImplementations() {
        CoreElement ce = this.taskDescription.getCoreElement();
        List<Implementation> coreImpls = ce.getImplementations();

        int coreImplsSize = coreImpls.size();
        Implementation[] impls = (Implementation[]) new Implementation[coreImplsSize];
        for (int i = 0; i < coreImplsSize; ++i) {
            impls[i] = (Implementation) coreImpls.get(i);
        }
        return impls;
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
        return true;
    }

    @Override
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return true;
    }

    @Override
    public boolean isToStopResource() {
        return false;
    }

    @Override
    public final <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore) {
        return targetWorker.generateResourceScore(this, this.taskDescription, actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // COMPUTE RESOURCE CANDIDATES
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates = new LinkedList<>();
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (compatibleWorkers.contains(target)) {
                candidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (compatibleWorkers.contains(target)) {
                    candidates.add(target);
                }
            }
        } else {
            // Free scheduling
            candidates = compatibleWorkers;
        }

        if (candidates.isEmpty()) {
            throw new BlockedActionException();
        }

        List prevExecutors = this.getExecutingResources();
        if (candidates.size() > prevExecutors.size() && prevExecutors.size() > 0) {
            candidates.removeAll(prevExecutors);
        }
        this.scheduleSecuredCandidates(actionScore, candidates);
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = this.getCompatibleWorkers();
        List<ResourceScheduler<? extends WorkerResourceDescription>> verifiedCandidates = new LinkedList<>();

        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> target = this.getEnforcedTargetResource();
            if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                verifiedCandidates.add(target);
            } else {
                throw new UnassignedActionException();
            }
        } else if (this.isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> target = a.getAssignedResource();
                if (candidates.contains(target) && compatibleWorkers.contains(target)) {
                    verifiedCandidates.add(target);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }

        } else {
            for (ResourceScheduler<? extends WorkerResourceDescription> candidate : candidates) {
                if (compatibleWorkers.contains(candidate) && compatibleWorkers.contains(candidate)) {
                    verifiedCandidates.add(candidate);
                }
            }
            if (verifiedCandidates.isEmpty()) {
                throw new UnassignedActionException();
            }
        }
        scheduleSecuredCandidates(actionScore, verifiedCandidates);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        if (targetWorker == null || !validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        Implementation bestImpl = null;
        Score bestScore = null;
        Score resourceScore = targetWorker.generateResourceScore(this, this.taskDescription, actionScore);
        if (resourceScore != null) {
            for (Implementation impl : getCompatibleImplementations(targetWorker)) {
                Score implScore =
                    targetWorker.generateImplementationScore(this, this.taskDescription, impl, resourceScore);
                if (Score.isBetter(implScore, bestScore)) {
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        if (bestImpl == null) {
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(targetWorker, bestImpl);
    }

    @Override
    public final void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {

        if (targetWorker == null || impl == null) {
            this.assignResource(null); // to remove previous allocation when re-scheduling the action
            throw new UnassignedActionException();
        }

        if (!validateWorker(targetWorker)) {
            throw new UnassignedActionException();
        }

        if (!targetWorker.getResource().canRun(impl)) {
            throw new UnassignedActionException();
        }

        assignWorkerAndImpl(targetWorker, impl);
    }

    /**
     * Verifies that the passed-in worker is able to run the task given the task execution history.
     * 
     * @param targetCandidate Resource whose aptitude to run the task has to be evaluated
     * @return {@literal true}, if the worker passed in is compatible with the action; {@literal false}, otherwise.
     */
    private boolean validateWorker(ResourceScheduler targetCandidate) {
        if (this.isTargetResourceEnforced()) {
            // The scheduling is forced to a given resource
            ResourceScheduler<? extends WorkerResourceDescription> enforcedTarget = this.getEnforcedTargetResource();
            if (enforcedTarget != targetCandidate) {
                return false;
            }
        } else if (this.isSchedulingConstrained()) {
            boolean isPredecessor = false;
            // The scheduling is constrained by dependencies
            for (AllocatableAction a : this.getConstrainingPredecessors()) {
                ResourceScheduler<? extends WorkerResourceDescription> predecessorHost = a.getAssignedResource();
                if (targetCandidate == predecessorHost) {
                    isPredecessor = true;
                }
            }
            if (!isPredecessor) {
                return false;
            }
        }

        if (this.getExecutingResources().contains(targetCandidate) && this.getCompatibleWorkers().size() > 1) {
            return false;
        }
        return true;
    }

    /**
     * Selects the best resource-Implementation pair given for an action given a score and a list of workers that could
     * potentially host the execution of the action. All the workers within the list have been already checked to be
     * able to host the action execution.
     * 
     * @param actionScore Score given by the scheduler to the action
     * @param candidates list of compatible workers that could host the action execution
     * @throws UnassignedActionException the action could not be assigned to any of the given candidate resources
     */
    private void scheduleSecuredCandidates(Score actionScore,
        List<ResourceScheduler<? extends WorkerResourceDescription>> candidates) throws UnassignedActionException {
        // COMPUTE BEST WORKER AND IMPLEMENTATION
        ResourceScheduler<? extends WorkerResourceDescription> bestWorker = null;
        Implementation bestImpl = null;
        Score bestScore = null;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : candidates) {
            Score resourceScore = worker.generateResourceScore(this, this.taskDescription, actionScore);
            if (resourceScore != null) {
                for (Implementation impl : getCompatibleImplementations(worker)) {
                    Score implScore =
                        worker.generateImplementationScore(this, this.taskDescription, impl, resourceScore);
                    if (Score.isBetter(implScore, bestScore)) {
                        bestWorker = worker;
                        bestImpl = impl;
                        bestScore = implScore;
                    }
                }
            }
        }

        if (bestWorker == null) {
            throw new UnassignedActionException();
        }
        assignWorkerAndImpl(bestWorker, bestImpl);
    }

    /**
     * Assigns an implementation and a worker to the action and submits the action scheduler to the corresponding
     * ResourceScheduler. The method assumes that the worker has already been checked to be a proper candidate to host
     * the action and the selected implementation is a compatible pick given the action, its requirements and the
     * worker's capabilities.
     *
     * @param worker worker where to submit the action
     * @param impl implementation to request the execution
     */
    private void assignWorkerAndImpl(ResourceScheduler worker, Implementation impl) {
        this.assignImplementation(impl);
        assignResource(worker);
        worker.scheduleAction(this);

    }

    @Override
    public Integer getCoreId() {
        return this.taskDescription.getCoreElement().getCoreId();
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public long getGroupPriority() {
        return ACTION_SINGLE;
    }

    @Override
    public OnFailure getOnFailure() {
        return OnFailure.RETRY;
    }

    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return false;
    }

    @Override
    protected void stopAction() throws Exception {
    }

}
