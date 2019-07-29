/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.util.LinkedList;
import java.util.List;


public class FakeAllocatableAction extends AllocatableAction {

    private final int id;
    private final int priority;
    private final List<Implementation> impls;


    /**
     * Creates a new FakeAllocatableAction.
     * 
     * @param orchestrator Associated action orchestrator.
     * @param id Action Id.
     * @param priority Whether the action has priority or not.
     * @param impls List of action implementations.
     */
    public FakeAllocatableAction(ActionOrchestrator orchestrator, int id, int priority, List<Implementation> impls) {
        super(new FullGraphSchedulingInformation(null), orchestrator);

        this.id = id;
        this.priority = priority;
        this.impls = impls;
    }

    @Override
    public void doAction() {
        this.profile.start();
    }

    @Override
    public void doCompleted() {
        this.profile.end();
    }

    @Override
    public void doError() throws FailedActionException {
    }

    @Override
    public void doFailed() {
    }

    @Override
    public String toString() {
        return "AllocatableAction " + id;
    }

    @Override
    public <T extends WorkerResourceDescription> List<Implementation> getCompatibleImplementations(
            ResourceScheduler<T> r) {

        List<Implementation> ret = new LinkedList<>();
        for (Implementation impl : this.impls) {
            ret.add((FakeImplementation) impl);
        }

        return ret;
    }

    @Override
    public List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        int implsSize = this.impls.size();
        FakeImplementation[] implementations = new FakeImplementation[implsSize];
        for (int i = 0; i < implsSize; ++i) {
            implementations[i] = (FakeImplementation) this.impls.get(i);
        }
        return implementations;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
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
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
            Score actionScore) {

        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // Nothing to do
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

        FakeImplementation impl = (FakeImplementation) impls.get(0);
        assignImplementation(impl);
        assignResource(targetWorker);
        targetWorker.scheduleAction(this);
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

        assignImplementation(impl);
        assignResource(targetWorker);
        targetWorker.scheduleAction(this);
    }

    public void selectExecution(ResourceScheduler<FakeResourceDescription> resource, FakeImplementation impl) {
        assignResource(resource);
        assignImplementation(impl);
    }

    /**
     * Dumps the dependencies description.
     * 
     * @return String containing the dependencies description.
     */
    public String dependenciesDescription() {
        StringBuilder sb = new StringBuilder("Action" + id + "\n");
        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) this.getSchedulingInfo();
        sb.append("\t depends on\n");
        sb.append("\t\tData : ").append(this.getDataPredecessors()).append("\n");
        sb.append("\t\tResource : ").append(dsi.getPredecessors()).append("\n");
        sb.append("\t enables\n");
        sb.append("\t\tData : ").append(this.getDataSuccessors()).append("\n");
        sb.append("\t\tResource : ").append(dsi.getSuccessors()).append("\n");

        return sb.toString();
    }

    @Override
    public Integer getCoreId() {
        if (impls == null || impls.size() == 0) {
            return null;
        } else {
            return impls.get(0).getCoreId();
        }
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean taskIsReadyForExecution() {
        return true;
    }

    @Override
    public boolean isToStopResource() {
        return false;
    }

    @Override
    protected void treatDependencyFreeAction(List<AllocatableAction> freeTasks) {
        // Nothing to do
    }

    @Override
    protected void doAbort() {
        // Nothing to do
    }

    @Override
    protected void doCanceled() {
        // Nothing to do
    }

    @Override
    protected void doFailIgnored() {
        // Nothing to do
    }

    @Override
    public OnFailure getOnFailure() {
        return null;
    }

    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return false;
    }

    @Override
    protected void doException(COMPSsException e) {
        // Nothing to do
    }

}
