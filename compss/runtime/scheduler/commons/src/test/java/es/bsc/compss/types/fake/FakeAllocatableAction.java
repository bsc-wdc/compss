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
package es.bsc.compss.types.fake;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
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

    private static int[] executions;
    private static int[] error;
    private static int[] failed;
    private static int[] cancelled;

    private int fakeId;


    /**
     * Creates a new FakeAllocatableAction instance.
     * 
     * @param td Associated task dispatcher.
     * @param id AllocatableAction id.
     */
    public FakeAllocatableAction(ActionOrchestrator td, int id) {
        super(new FakeSI(null), td);
        this.fakeId = id;
    }

    /**
     * Resizes the FakeAllocatableAction pool.
     * 
     * @param size New pool size.
     */
    public static void resize(int size) {
        FakeAllocatableAction.executions = new int[size];
        FakeAllocatableAction.error = new int[size];
        FakeAllocatableAction.failed = new int[size];
        FakeAllocatableAction.cancelled = new int[size];
    }

    public static int getSize() {
        return FakeAllocatableAction.executions.length;
    }

    public static int getExecution(int id) {
        return FakeAllocatableAction.executions[id];
    }

    public static int getError(int id) {
        return FakeAllocatableAction.error[id];
    }

    public static int getFailed(int id) {
        return FakeAllocatableAction.failed[id];
    }

    public static int getCancelled(int id) {
        return FakeAllocatableAction.cancelled[id];
    }

    public int getFakeId() {
        return this.fakeId;
    }

    @Override
    public void doAction() {
        executions[this.fakeId]++;
    }

    @Override
    protected void doAbort() {
        // Nothing to do
    }

    @Override
    public void doCompleted() {
        // Nothing to do
    }

    @Override
    public void doError() throws FailedActionException {
        error[this.fakeId]++;
        if (error[this.fakeId] == 2) {
            throw new FailedActionException();
        }
    }

    @Override
    public void doFailed() {
        failed[this.fakeId]++;
    }

    @Override
    protected boolean doCanceled() {
        cancelled[this.fakeId]++;
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
    public <T extends WorkerResourceDescription> LinkedList<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r) {

        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        return new FakeImplementation[0];
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
        return true;
    }

    @Override
    public boolean isToReserveResources() {
        return false;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    public boolean isToStopResource() {
        return false;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        // Nothing to do
    }

    @Override
    public void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException {
        // Nothing to do
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Score actionScore)
        throws UnassignedActionException {
        // Nothing to do
    }

    @Override
    public void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker, Implementation impl)
        throws UnassignedActionException {
        // Nothing to do
    }

    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore) {

        return null;
    }

    @Override
    public Integer getCoreId() {
        return null;
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
