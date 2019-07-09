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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;


public class OptimizationAction extends AllocatableAction {

    public OptimizationAction(ActionOrchestrator orchestrator) {
        super(new FullGraphSchedulingInformation(null), orchestrator);
    }

    @Override
    protected boolean areEnoughResources() {
        return true;
    }

    @Override
    protected void reserveResources() {
        // Nothing to do
    }

    @Override
    protected void releaseResources() {
        // Nothing to do
    }

    @Override
    protected void doAction() {
        // Nothing to do
    }

    @Override
    protected void doCompleted() {
        // Nothing to do
    }

    @Override
    protected void doError() throws FailedActionException {
        // Nothing to do
    }

    @Override
    protected void doFailed() {
        // Nothing to do
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation[] getImplementations() {
        return null;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return true;
    }

    @Override
    public <T extends WorkerResourceDescription> List<Implementation> getCompatibleImplementations(
            ResourceScheduler<T> r) {

        return null;
    }

    @Override
    public int getPriority() {
        return 0;
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

        // Nothing to do
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

        // Nothing to do
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
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
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
